from __future__ import annotations

"""Authoritative stateful LTF execution model for active TPO watches.

The v2 model deliberately separates auction permission from execution timing:

TPO Watch Bridge
    -> ARMED
    -> IMPULSE_DETECTED
    -> BOS_CONFIRMED
    -> RETEST_PENDING
    -> RETEST_TOUCHED
    -> RETEST_HELD
    -> TRIGGER_CONFIRMED
    -> ENTRY_READY

Only ENTRY_READY may expose READY + EXECUTABLE geometry.  A BOS is never treated
as a retest, timestamps are causal, and an entry is anchored to the retest
level rather than following the latest close.
"""

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd


LTF_EXECUTION_STATE_MACHINE_VERSION = (
    "ltf-execution-state-machine-v2.0-causal-m5-retest"
)
LTF_EXECUTION_STATE_SCHEMA_VERSION = "2.0"

STATE_ARMED = "ARMED"
STATE_IMPULSE_DETECTED = "IMPULSE_DETECTED"
STATE_BOS_CONFIRMED = "BOS_CONFIRMED"
STATE_RETEST_PENDING = "RETEST_PENDING"
STATE_RETEST_TOUCHED = "RETEST_TOUCHED"
STATE_RETEST_HELD = "RETEST_HELD"
STATE_TRIGGER_CONFIRMED = "TRIGGER_CONFIRMED"
STATE_ENTRY_READY = "ENTRY_READY"
STATE_INVALIDATED = "INVALIDATED"
STATE_EXPIRED = "EXPIRED"
STATE_MISSED = "MISSED"
STATE_CONTEXT_CANCELLED = "CONTEXT_CANCELLED"

TERMINAL_STATES = {
    STATE_INVALIDATED,
    STATE_EXPIRED,
    STATE_MISSED,
    STATE_CONTEXT_CANCELLED,
}

ACTIVE_TPO_WATCH_STATES = {"LTF_MODEL_PENDING", "LTF_MODEL_CONFIRMED"}
SUPPORTED_WATCH_SETUPS = {
    "OPEN_DRIVE",
    "OPEN_DRIVE_CONFIRMED",
    "OPEN_TEST_DRIVE",
    "OPEN_TEST_DRIVE_CANDIDATE",
    "OPEN_TEST_DRIVE_CONFIRMED",
    "OPEN_REJECTION_REVERSE",
    "OPEN_AUCTION_ACCEPTED_BREAKOUT",
    "OPEN_AUCTION_OUT_OF_RANGE_ACCEPTED_BREAKOUT",
    "OPEN_AUCTION_OUT_OF_RANGE_FAILED_ACCEPTANCE",
    "OPEN_AUCTION_FAILED_ACCEPTANCE",
    "FAILED_ACCEPTANCE_BACK_TO_VALUE",
}

VALUE_ACCEPTANCE_INVALIDATES_OTD = {
    "ACCEPTED_BACK_INSIDE_VALUE",
    "ACCEPTED_INSIDE_VALUE",
    "VALUE_ACCEPTED_INSIDE",
    "FAILED_OUTSIDE_VALUE",
}

MIN_CONFIRMED_RR = 2.0
DEFAULT_SETUP_TTL_MINUTES = 240
DEFAULT_ENTRY_WINDOW_TTL_MINUTES = 30
DEFAULT_STRUCTURE_LOOKBACK = 5
DEFAULT_MIN_HISTORY_BARS = 8
DEFAULT_MAX_TRIGGER_EXTENSION_R = 0.70
DEFAULT_TERMINAL_RETENTION_DAYS = 14

TICK_SIZE_BY_SYMBOL: dict[str, float] = {
    "XAUUSD": 0.10,
    "BTCUSD": 1.0,
    "ETHUSD": 0.10,
    "GER40": 1.0,
    "NAS100": 1.0,
    "SPX500": 0.25,
    "UKOIL": 0.01,
    "USDJPY": 0.001,
    "EURUSD": 0.00001,
    "GBPUSD": 0.00001,
    "USDCHF": 0.00001,
    "USDCAD": 0.00001,
    "AUDUSD": 0.00001,
}

MIN_STOP_BY_SYMBOL: dict[str, float] = {
    # Use the most conservative existing SignalTracker/Telegram threshold so
    # v2 can never rehydrate READY geometry that a downstream TIGHT_STOP guard
    # would correctly reject.
    "XAUUSD": 15.0,
    "BTCUSD": 100.0,
    "ETHUSD": 10.0,
    "GER40": 30.0,
    "NAS100": 50.0,
    "SPX500": 8.0,
    "UKOIL": 0.25,
    "USDJPY": 0.08,
    "EURUSD": 0.0005,
    "GBPUSD": 0.0007,
    "USDCHF": 0.0005,
    "USDCAD": 0.0007,
    "AUDUSD": 0.0005,
}

SETUP_PROFILES: dict[str, dict[str, Any]] = {
    "OPEN_DRIVE": {
        "scenario_prefix": "TPO_OPEN_DRIVE",
        "model": "PULLBACK_CONTINUATION",
        "confidence": 0.72,
        "requires_caution": False,
    },
    "OPEN_TEST_DRIVE": {
        "scenario_prefix": "TPO_OPEN_TEST_DRIVE",
        "model": "FAILED_ACCEPTANCE_RETEST",
        "confidence": 0.68,
        "requires_caution": False,
    },
    "OPEN_REJECTION_REVERSE": {
        "scenario_prefix": "TPO_OPEN_REJECTION_REVERSE",
        "model": "RECLAIM_BOS_RETEST",
        "confidence": 0.66,
        "requires_caution": True,
    },
    "OPEN_AUCTION_BREAKOUT": {
        "scenario_prefix": "TPO_OPEN_AUCTION_BREAKOUT",
        "model": "ACCEPTED_BREAKOUT_RETEST",
        "confidence": 0.67,
        "requires_caution": False,
    },
    "OPEN_AUCTION_BACK_TO_VALUE": {
        "scenario_prefix": "TPO_OPEN_AUCTION_BACK_TO_VALUE",
        "model": "FAILED_ACCEPTANCE_BACK_TO_VALUE",
        "confidence": 0.65,
        "requires_caution": True,
    },
}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _upper(value: Any, default: str = "") -> str:
    raw = getattr(value, "value", value)
    text = str(raw or "").strip().upper()
    return text or default


def _raw_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "confirmed", "valid", "ok"}:
        return True
    if text in {"0", "false", "no", "n", "off", "none", "null", ""}:
        return False
    return default


def _as_utc(value: Any, default: datetime | None = None) -> datetime | None:
    if value in (None, ""):
        return default
    try:
        stamp = pd.Timestamp(value)
    except Exception:
        return default
    if pd.isna(stamp):
        return default
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.to_pydatetime()


def _iso(value: Any) -> str | None:
    parsed = _as_utc(value)
    return parsed.isoformat() if parsed is not None else None


def _nested_sources(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    sources: list[Mapping[str, Any]] = [payload]
    queue: list[Any] = [
        payload.get("metadata"),
        payload.get("auction_context"),
        payload.get("auction_filters"),
        payload.get("tpo"),
        payload.get("tpo_context"),
        payload.get("open_behavior"),
        payload.get("open_context"),
        payload.get("session_context"),
        payload.get("execution"),
        payload.get("execution_plan"),
    ]
    seen: set[int] = {id(payload)}
    while queue:
        item = queue.pop(0)
        if not isinstance(item, Mapping) or id(item) in seen:
            continue
        seen.add(id(item))
        sources.append(item)
        for key in (
            "metadata",
            "context",
            "filters",
            "open_behavior",
            "session_context",
            "execution",
        ):
            queue.append(item.get(key))
    return sources


def _first_value(payload: Mapping[str, Any], *keys: str) -> Any:
    for source in _nested_sources(payload):
        for key in keys:
            value = source.get(key)
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return value
    return None


def _first_float(payload: Mapping[str, Any], *keys: str) -> float | None:
    for source in _nested_sources(payload):
        for key in keys:
            value = _float(source.get(key))
            if value is not None:
                return value
    return None


def _symbol(payload: Mapping[str, Any]) -> str:
    return _upper(_first_value(payload, "symbol", "instrument"), "-")


def _family_from_behavior(value: Any) -> str | None:
    behavior = _upper(value)
    if behavior in {"OPEN_DRIVE", "OPEN_DRIVE_CONFIRMED"}:
        return "OPEN_DRIVE"
    if behavior in {
        "OPEN_TEST_DRIVE",
        "OPEN_TEST_DRIVE_CANDIDATE",
        "OPEN_TEST_DRIVE_CONFIRMED",
    }:
        return "OPEN_TEST_DRIVE"
    if behavior == "OPEN_REJECTION_REVERSE":
        return "OPEN_REJECTION_REVERSE"
    if behavior in {
        "OPEN_AUCTION_ACCEPTED_BREAKOUT",
        "OPEN_AUCTION_OUT_OF_RANGE_ACCEPTED_BREAKOUT",
    }:
        return "OPEN_AUCTION_BREAKOUT"
    if behavior in {
        "OPEN_AUCTION_OUT_OF_RANGE_FAILED_ACCEPTANCE",
        "OPEN_AUCTION_FAILED_ACCEPTANCE",
        "FAILED_ACCEPTANCE_BACK_TO_VALUE",
    }:
        return "OPEN_AUCTION_BACK_TO_VALUE"
    return None


def _canonical_setup_family(payload: Mapping[str, Any]) -> str | None:
    # Preserve the classifier/Watch Bridge precedence contract.  A stale broad
    # legacy ``open_behavior`` must never override the authoritative current
    # branch selected by ``tpo_watch_setup`` or ``current_open_behavior``.
    for value in (
        _first_value(payload, "tpo_watch_setup"),
        _first_value(payload, "current_open_behavior", "updated_open_behavior"),
    ):
        family = _family_from_behavior(value)
        if family is not None:
            return family

    value_state = _upper(_first_value(payload, "value_acceptance_state"))
    if value_state in {
        "ACCEPTED_OUTSIDE_VALUE",
        "ACCEPTED_OUTSIDE_PRIOR_VALUE",
        "ACCEPTED_OUTSIDE_RANGE",
        "ACCEPTED_OUTSIDE_PRIOR_RANGE",
        "ACCEPTED_ABOVE_VALUE",
        "ACCEPTED_BELOW_VALUE",
        "ACCEPTED_BREAKOUT",
        "ACCEPTED_EXTENSION",
        "IB_ACCEPTED_EXTENSION",
    }:
        return "OPEN_AUCTION_BREAKOUT"
    if value_state in {
        "REJECTED_BACK_INTO_PRIOR_VALUE",
        "REJECTED_BACK_INTO_PRIOR_RANGE",
        "FAILED_ACCEPTANCE_INTO_PRIOR_VALUE",
        "FAILED_ACCEPTANCE_INTO_PRIOR_RANGE",
        "ACCEPTED_BACK_INSIDE_VALUE",
        "ACCEPTED_INSIDE_VALUE",
        "VALUE_ACCEPTED_INSIDE",
        "FAILED_OUTSIDE_VALUE",
    }:
        return "OPEN_AUCTION_BACK_TO_VALUE"

    for value in (
        _first_value(payload, "open_behavior"),
        _first_value(payload, "initial_open_behavior"),
    ):
        family = _family_from_behavior(value)
        if family is not None:
            return family
    return None


def _direction(payload: Mapping[str, Any], setup_family: str | None) -> str | None:
    direct = _upper(
        _first_value(
            payload,
            "direction",
            "expected_direction",
            "entry_window_direction",
            "raw_direction",
            "battle_bias_hint",
        )
    )
    if direct in {"LONG", "BUY", "BULL", "BULLISH", "UP"}:
        return "LONG"
    if direct in {"SHORT", "SELL", "BEAR", "BEARISH", "DOWN"}:
        return "SHORT"

    text = " ".join(
        _upper(_first_value(payload, key))
        for key in (
            "scenario",
            "scenario_type",
            "trigger_reason",
            "entry_model_hint",
            "auction_bias",
            "htf_bias",
        )
    )
    if any(token in text for token in ("_LONG", " LONG", "BULLISH", "BUY")):
        return "LONG"
    if any(token in text for token in ("_SHORT", " SHORT", "BEARISH", "SELL")):
        return "SHORT"

    location = _upper(_first_value(payload, "open_location", "open_relation"))
    above = any(token in location for token in ("ABOVE", "OUT_OF_RANGE_HIGH"))
    below = any(token in location for token in ("BELOW", "OUT_OF_RANGE_LOW"))
    reverse_family = setup_family in {
        "OPEN_REJECTION_REVERSE",
        "OPEN_AUCTION_BACK_TO_VALUE",
    }
    if above:
        return "SHORT" if reverse_family else "LONG"
    if below:
        return "LONG" if reverse_family else "SHORT"
    return None


def is_active_tpo_watch(payload: Mapping[str, Any]) -> tuple[bool, str]:
    state = _upper(_first_value(payload, "tpo_watch_state"))
    active = _bool(_first_value(payload, "tpo_watch_active"), False)
    setup = _upper(_first_value(payload, "tpo_watch_setup"))
    family = _canonical_setup_family(payload)
    value_state = _upper(_first_value(payload, "value_acceptance_state"))
    value_rejection = _bool(
        _first_value(payload, "value_rejection_confirmed"),
        False,
    )

    if state not in ACTIVE_TPO_WATCH_STATES:
        return False, f"tpo_watch_state_not_active:{state or 'missing'}"
    if not active:
        return False, "tpo_watch_not_active"
    if setup and setup not in SUPPORTED_WATCH_SETUPS and family is None:
        return False, f"unsupported_tpo_watch_setup:{setup}"
    if family is None:
        return False, "unsupported_tpo_watch_family"
    if (
        family == "OPEN_TEST_DRIVE"
        and value_state in VALUE_ACCEPTANCE_INVALIDATES_OTD
        and not value_rejection
    ):
        return False, f"otd_invalidated_by_value_acceptance:{value_state}"
    return True, "active_tpo_watch"


def _tick_size(symbol: str, price: float | None = None) -> float:
    configured = TICK_SIZE_BY_SYMBOL.get(symbol)
    if configured is not None:
        return configured
    if price is not None and price > 0:
        return max(price * 0.00001, 0.00001)
    return 0.00001


def _min_stop(symbol: str, price: float) -> float:
    configured = MIN_STOP_BY_SYMBOL.get(symbol)
    if configured is not None:
        return configured
    return max(abs(price) * 0.0002, _tick_size(symbol, price) * 3.0)


def _collect_levels(payload: Mapping[str, Any]) -> dict[str, float]:
    aliases: dict[str, tuple[str, ...]] = {
        "previous_high": (
            "previous_high",
            "prior_high",
            "prev_high",
            "pd_high",
            "session_previous_high",
        ),
        "previous_low": (
            "previous_low",
            "prior_low",
            "prev_low",
            "pd_low",
            "session_previous_low",
        ),
        "range_high": ("range_high", "balance_high", "ib_high", "initial_balance_high"),
        "range_low": ("range_low", "balance_low", "ib_low", "initial_balance_low"),
        "vah": ("vah", "VAH", "value_area_high", "prior_vah", "previous_vah"),
        "val": ("val", "VAL", "value_area_low", "prior_val", "previous_val"),
        "poc": ("poc", "POC", "prior_poc", "previous_poc", "naked_poc", "npoc"),
        "open_price": ("open_price", "current_open", "session_open", "market_open"),
        "breakout_level": (
            "breakout_level",
            "base_level",
            "retest_level",
            "trigger_level",
        ),
        "liquidity_level": ("liquidity_level", "sweep_level", "reclaim_level"),
        "context_invalidation": (
            "context_invalidation_price",
            "context_invalidation_level",
            "invalidation_reference_price",
            "invalidation_level",
        ),
    }
    levels: dict[str, float] = {}
    for canonical, keys in aliases.items():
        value = _first_float(payload, *keys)
        if value is not None:
            levels[canonical] = value
    return levels


def _reference_level(
    payload: Mapping[str, Any],
    *,
    setup_family: str,
    direction: str,
    levels: Mapping[str, float],
) -> tuple[float | None, str | None]:
    explicit = _first_float(
        payload,
        "value_test_price",
        "interest_zone_price",
        "primary_interest_zone_price",
        "retest_level",
        "breakout_level",
    )
    if explicit is not None:
        return explicit, "explicit_reference"

    test_level = _upper(_first_value(payload, "value_test_level", "tested_level"))
    if test_level in {"VAH", "VAL", "POC"}:
        value = levels.get(test_level.lower())
        if value is not None:
            return value, test_level

    if setup_family == "OPEN_TEST_DRIVE":
        key = "vah" if direction == "LONG" else "val"
        if levels.get(key) is not None:
            return levels[key], key.upper()

    if setup_family == "OPEN_AUCTION_BACK_TO_VALUE":
        key = "val" if direction == "LONG" else "vah"
        if levels.get(key) is not None:
            return levels[key], key.upper()

    if setup_family == "OPEN_REJECTION_REVERSE":
        for key in (
            ("previous_low", "range_low", "val")
            if direction == "LONG"
            else ("previous_high", "range_high", "vah")
        ):
            if levels.get(key) is not None:
                return levels[key], key.upper()

    if setup_family in {"OPEN_DRIVE", "OPEN_AUCTION_BREAKOUT"}:
        for key in (
            ("breakout_level", "previous_high", "range_high", "vah", "poc")
            if direction == "LONG"
            else ("breakout_level", "previous_low", "range_low", "val", "poc")
        ):
            if levels.get(key) is not None:
                return levels[key], key.upper()
    return None, None


def _setup_anchor(payload: Mapping[str, Any], as_of: datetime) -> str:
    explicit = _first_value(
        payload,
        "current_session_id",
        "session_id",
        "open_event",
        "reference_profile_id",
    )
    if explicit not in (None, ""):
        return str(explicit)
    return as_of.date().isoformat()


def _setup_id(
    *,
    symbol: str,
    direction: str,
    setup_family: str,
    anchor: str,
    reference_profile_id: str | None,
) -> str:
    raw = "|".join(
        (
            symbol,
            direction,
            setup_family,
            anchor,
            reference_profile_id or "-",
        )
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"LTFV2_{symbol}_{digest}"


def _normalize_bars(
    df: pd.DataFrame | None,
    *,
    as_of: datetime,
) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out.columns = [str(column).lower() for column in out.columns]
    required = {"open", "high", "low", "close"}
    if required.difference(out.columns):
        return pd.DataFrame()

    if "bar_open_utc" in out.columns:
        opens = pd.to_datetime(out["bar_open_utc"], utc=True, errors="coerce")
    elif "datetime" in out.columns:
        opens = pd.to_datetime(out["datetime"], utc=True, errors="coerce")
    elif "timestamp" in out.columns:
        opens = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    else:
        opens = pd.to_datetime(out.index, utc=True, errors="coerce")

    out["bar_open_utc"] = opens
    if "bar_close_utc" in out.columns:
        closes = pd.to_datetime(out["bar_close_utc"], utc=True, errors="coerce")
    else:
        closes = opens + pd.Timedelta(minutes=5)
    out["bar_close_utc"] = closes

    for column in ("open", "high", "low", "close"):
        out[column] = pd.to_numeric(out[column], errors="coerce")
    if "volume" not in out.columns:
        out["volume"] = 0.0
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0.0)

    out = out.dropna(
        subset=[
            "bar_open_utc",
            "bar_close_utc",
            "open",
            "high",
            "low",
            "close",
        ]
    )
    out = out.loc[out["bar_close_utc"] <= pd.Timestamp(as_of)]
    out = out.sort_values("bar_open_utc")
    out = out.drop_duplicates(subset=["bar_open_utc"], keep="last")
    return out.reset_index(drop=True)


def _median_range(history: pd.DataFrame) -> float:
    if history.empty:
        return 0.0
    ranges = (history["high"] - history["low"]).abs()
    value = float(ranges.tail(20).median())
    return value if math.isfinite(value) and value > 0 else 0.0


def _median_body(history: pd.DataFrame) -> float:
    if history.empty:
        return 0.0
    bodies = (history["close"] - history["open"]).abs()
    value = float(bodies.tail(20).median())
    return value if math.isfinite(value) and value > 0 else 0.0


def _atr(history: pd.DataFrame) -> float:
    if history.empty:
        return 0.0
    frame = history.tail(21)
    previous_close = frame["close"].shift(1)
    true_range = pd.concat(
        (
            (frame["high"] - frame["low"]).abs(),
            (frame["high"] - previous_close).abs(),
            (frame["low"] - previous_close).abs(),
        ),
        axis=1,
    ).max(axis=1)
    value = float(true_range.tail(20).median())
    if math.isfinite(value) and value > 0:
        return value
    return _median_range(frame)


def _bar_snapshot(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "bar_open_utc": _iso(row.get("bar_open_utc")),
        "bar_close_utc": _iso(row.get("bar_close_utc")),
        "open": round(float(row["open"]), 8),
        "high": round(float(row["high"]), 8),
        "low": round(float(row["low"]), 8),
        "close": round(float(row["close"]), 8),
    }


def _append_transition(
    setup: dict[str, Any],
    state: str,
    *,
    at_utc: Any,
    reason: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    setup["state"] = state
    setup["updated_at_utc"] = _iso(at_utc)
    event = {
        "state": state,
        "at_utc": _iso(at_utc),
        "reason": reason,
    }
    if details:
        event["details"] = dict(details)
    history = setup.setdefault("transition_history", [])
    if history and history[-1].get("state") == state and history[-1].get("at_utc") == event["at_utc"]:
        return
    history.append(event)
    if len(history) > 64:
        del history[:-64]


def _create_setup(
    payload: Mapping[str, Any],
    *,
    as_of: datetime,
    symbol: str,
    direction: str,
    setup_family: str,
    setup_id: str,
    levels: Mapping[str, float],
    reference_level: float | None,
    reference_label: str | None,
) -> dict[str, Any]:
    explicit_expiry = _as_utc(
        _first_value(
            payload,
            "expires_at_utc",
            "setup_expires_at_utc",
            "watch_expires_at_utc",
        )
    )
    ttl_minutes = _env_int("LTF_V2_SETUP_TTL_MINUTES", DEFAULT_SETUP_TTL_MINUTES)
    expiry = explicit_expiry or (as_of + timedelta(minutes=max(15, ttl_minutes)))
    if expiry <= as_of:
        expiry = as_of + timedelta(minutes=max(15, ttl_minutes))

    explicit_invalidation = levels.get("context_invalidation")
    setup: dict[str, Any] = {
        "version": LTF_EXECUTION_STATE_MACHINE_VERSION,
        "setup_id": setup_id,
        "symbol": symbol,
        "direction": direction,
        "setup_family": setup_family,
        "state": STATE_ARMED,
        "armed_at_utc": as_of.isoformat(),
        "updated_at_utc": as_of.isoformat(),
        "expires_at_utc": expiry.isoformat(),
        "last_processed_bar_utc": as_of.isoformat(),
        "reference_profile_id": _raw_text(
            _first_value(payload, "reference_profile_id")
        ),
        "session_scope": _upper(_first_value(payload, "session_scope")) or None,
        "primary_session": _upper(_first_value(payload, "primary_session")) or None,
        "current_open_behavior": _upper(
            _first_value(payload, "current_open_behavior", "updated_open_behavior")
        )
        or None,
        "value_acceptance_state": _upper(
            _first_value(payload, "value_acceptance_state")
        )
        or None,
        "reference_level": reference_level,
        "reference_level_label": reference_label,
        "context_invalidation_price": explicit_invalidation,
        "levels_at_arm": dict(levels),
        "bos_at_utc": None,
        "bos_level": None,
        "impulse_extreme": None,
        "atr_at_bos": None,
        "retest_zone_low": None,
        "retest_zone_high": None,
        "retest_at_utc": None,
        "retest_extreme": None,
        "retest_bar_high": None,
        "retest_bar_low": None,
        "trigger_at_utc": None,
        "trigger_price": None,
        "entry_ready_at_utc": None,
        "entry_window_expires_at_utc": None,
        "entry_reference_price": None,
        "invalidation_reference_price": None,
        "target_reference_price": None,
        "risk_reward_ratio": None,
        "target_source": None,
        "target_zone_type": None,
        "target_zone_role": None,
        "geometry_outcome": None,
        "terminal_reason": None,
        "transition_history": [],
    }
    _append_transition(
        setup,
        STATE_ARMED,
        at_utc=as_of,
        reason="active_tpo_watch_armed",
        details={
            "setup_family": setup_family,
            "direction": direction,
            "reference_level": reference_level,
        },
    )
    return setup


def _is_context_invalidated(
    setup: Mapping[str, Any],
    row: Mapping[str, Any],
    *,
    atr_value: float,
) -> bool:
    direction = str(setup["direction"])
    explicit = _float(setup.get("context_invalidation_price"))
    bos_level = _float(setup.get("bos_level"))
    tolerance = max(
        _tick_size(str(setup["symbol"]), bos_level) * 2.0,
        atr_value * 0.10,
    )

    if explicit is not None:
        if direction == "LONG" and float(row["close"]) < explicit - tolerance:
            return True
        if direction == "SHORT" and float(row["close"]) > explicit + tolerance:
            return True

    if bos_level is None or atr_value <= 0:
        return False
    structural_buffer = max(atr_value, tolerance * 4.0)
    if direction == "LONG":
        return float(row["close"]) < bos_level - structural_buffer
    return float(row["close"]) > bos_level + structural_buffer


def _detect_bos(
    *,
    setup: Mapping[str, Any],
    row: Mapping[str, Any],
    history: pd.DataFrame,
) -> dict[str, Any] | None:
    lookback = _env_int("LTF_V2_STRUCTURE_LOOKBACK", DEFAULT_STRUCTURE_LOOKBACK)
    previous = history.tail(max(3, lookback))
    if len(previous) < max(3, lookback):
        return None

    direction = str(setup["direction"])
    symbol = str(setup["symbol"])
    current_open = float(row["open"])
    current_high = float(row["high"])
    current_low = float(row["low"])
    current_close = float(row["close"])
    current_range = max(current_high - current_low, 0.0)
    current_body = abs(current_close - current_open)
    median_range = _median_range(previous)
    median_body = _median_body(previous)
    atr_value = _atr(pd.concat((previous, pd.DataFrame([row]))))
    tick = _tick_size(symbol, current_close)
    break_buffer = max(tick * 2.0, atr_value * 0.05)

    if current_range <= 0:
        return None
    body_ok = current_body >= max(median_body * 1.20, median_range * 0.35)
    range_ok = current_range >= max(median_range * 1.05, tick * 4.0)
    if not body_ok or not range_ok:
        return None

    if direction == "LONG":
        structure = float(previous["high"].max())
        close_location = (current_close - current_low) / current_range
        confirmed = (
            current_close > structure + break_buffer
            and current_close > current_open
            and close_location >= 0.65
        )
        impulse_extreme = current_high
    else:
        structure = float(previous["low"].min())
        close_location = (current_high - current_close) / current_range
        confirmed = (
            current_close < structure - break_buffer
            and current_close < current_open
            and close_location >= 0.65
        )
        impulse_extreme = current_low

    if not confirmed:
        return None
    zone_half_width = max(tick * 2.0, atr_value * 0.15)
    return {
        "bos_level": structure,
        "impulse_extreme": impulse_extreme,
        "atr": atr_value,
        "zone_low": structure - zone_half_width,
        "zone_high": structure + zone_half_width,
        "break_buffer": break_buffer,
        "bar": _bar_snapshot(row),
    }


def _touches_retest_zone(setup: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
    zone_low = _float(setup.get("retest_zone_low"))
    zone_high = _float(setup.get("retest_zone_high"))
    if zone_low is None or zone_high is None:
        return False
    return float(row["low"]) <= zone_high and float(row["high"]) >= zone_low


def _retest_holds(setup: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
    bos_level = _float(setup.get("bos_level"))
    if bos_level is None:
        return False
    if setup["direction"] == "LONG":
        return float(row["close"]) >= bos_level and float(row["close"]) > float(row["open"])
    return float(row["close"]) <= bos_level and float(row["close"]) < float(row["open"])


def _trigger_confirms(
    setup: Mapping[str, Any],
    row: Mapping[str, Any],
    history: pd.DataFrame,
) -> bool:
    direction = str(setup["direction"])
    atr_value = _atr(history)
    tick = _tick_size(str(setup["symbol"]), float(row["close"]))
    buffer = max(tick, atr_value * 0.03)
    body = abs(float(row["close"]) - float(row["open"]))
    current_range = max(float(row["high"]) - float(row["low"]), 0.0)
    median_body = _median_body(history.iloc[:-1] if len(history) > 1 else history)
    if current_range <= 0 or body < max(median_body * 0.70, tick * 2.0):
        return False

    if direction == "LONG":
        level = _float(setup.get("retest_bar_high"))
        if level is None:
            return False
        close_location = (float(row["close"]) - float(row["low"])) / current_range
        return (
            float(row["close"]) > level + buffer
            and float(row["close"]) > float(row["open"])
            and close_location >= 0.60
        )

    level = _float(setup.get("retest_bar_low"))
    if level is None:
        return False
    close_location = (float(row["high"]) - float(row["close"])) / current_range
    return (
        float(row["close"]) < level - buffer
        and float(row["close"]) < float(row["open"])
        and close_location >= 0.60
    )


def _append_target_candidate(
    candidates: list[dict[str, Any]],
    *,
    price: Any,
    zone_type: Any,
    role: Any,
    source: str,
) -> None:
    value = _float(price)
    if value is None:
        return
    candidates.append(
        {
            "price": value,
            "zone_type": _upper(zone_type) or None,
            "zone_role": _upper(role) or None,
            "source": source,
        }
    )


def _target_candidates(
    payload: Mapping[str, Any],
    levels: Mapping[str, float],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for source_index, source in enumerate(_nested_sources(payload)):
        for key in ("primary_interest_zone", "interest_zone"):
            zone = source.get(key)
            if isinstance(zone, Mapping):
                _append_target_candidate(
                    candidates,
                    price=zone.get("price") or zone.get("level") or zone.get("value"),
                    zone_type=zone.get("zone_type") or zone.get("type") or zone.get("name"),
                    role=zone.get("role") or zone.get("zone_role"),
                    source=f"source[{source_index}].{key}",
                )
        zones = source.get("interest_zones")
        if isinstance(zones, Sequence) and not isinstance(
            zones,
            (str, bytes, bytearray),
        ):
            for index, zone in enumerate(zones):
                if not isinstance(zone, Mapping):
                    continue
                _append_target_candidate(
                    candidates,
                    price=zone.get("price") or zone.get("level") or zone.get("value"),
                    zone_type=zone.get("zone_type") or zone.get("type") or zone.get("name"),
                    role=zone.get("role") or zone.get("zone_role"),
                    source=f"source[{source_index}].interest_zones[{index}]",
                )

    for key, zone_type in (
        ("previous_high", "PREVIOUS_HIGH"),
        ("previous_low", "PREVIOUS_LOW"),
        ("range_high", "IB_HIGH"),
        ("range_low", "IB_LOW"),
        ("vah", "VAH"),
        ("val", "VAL"),
        ("poc", "POC"),
    ):
        if key in levels:
            _append_target_candidate(
                candidates,
                price=levels[key],
                zone_type=zone_type,
                role="STRUCTURAL_INTEREST",
                source=f"normalized_level.{key}",
            )

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[float, str | None]] = set()
    for candidate in candidates:
        key = (round(float(candidate["price"]), 8), candidate.get("zone_type"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _build_geometry(
    setup: dict[str, Any],
    *,
    payload: Mapping[str, Any],
    current_price: float,
    levels: Mapping[str, float],
) -> dict[str, Any]:
    symbol = str(setup["symbol"])
    direction = str(setup["direction"])
    entry = _float(setup.get("bos_level"))
    retest_extreme = _float(setup.get("retest_extreme"))
    atr_value = _float(setup.get("atr_at_bos")) or 0.0
    explicit_invalidation = _float(setup.get("context_invalidation_price"))
    if entry is None or retest_extreme is None:
        return {"outcome": "CONFIRMED_MISSING_RETEST_GEOMETRY"}

    tick = _tick_size(symbol, entry)
    stop_buffer = max(tick * 2.0, atr_value * 0.10)
    minimum_stop = _min_stop(symbol, entry)
    if direction == "LONG":
        stop = min(retest_extreme - stop_buffer, entry - minimum_stop)
        if explicit_invalidation is not None and explicit_invalidation < entry:
            stop = min(stop, explicit_invalidation - stop_buffer)
        risk = entry - stop
    else:
        stop = max(retest_extreme + stop_buffer, entry + minimum_stop)
        if explicit_invalidation is not None and explicit_invalidation > entry:
            stop = max(stop, explicit_invalidation + stop_buffer)
        risk = stop - entry

    if risk <= 0 or not math.isfinite(risk):
        return {"outcome": "CONFIRMED_INVALID_STOP_GEOMETRY"}

    candidates: list[dict[str, Any]] = []
    for candidate in _target_candidates(payload, levels):
        price = float(candidate["price"])
        if direction == "LONG":
            reward = price - entry
        else:
            reward = entry - price
        if reward <= 0:
            continue
        enriched = dict(candidate)
        enriched["distance"] = reward
        enriched["rr"] = reward / risk
        candidates.append(enriched)

    candidates.sort(key=lambda item: (float(item["distance"]), str(item.get("zone_type"))))
    satisfying = [
        candidate
        for candidate in candidates
        if float(candidate["rr"]) >= MIN_CONFIRMED_RR
    ]
    selected = satisfying[0] if satisfying else (candidates[0] if candidates else None)
    if selected is None:
        return {
            "outcome": "CONFIRMED_NEEDS_REAL_TARGET",
            "entry_reference_price": entry,
            "invalidation_reference_price": stop,
            "stop_distance": risk,
            "target_candidates": [],
        }

    target = float(selected["price"])
    reward = float(selected["distance"])
    rr = reward / risk
    already_moved_r = (
        max(0.0, (current_price - entry) / risk)
        if direction == "LONG"
        else max(0.0, (entry - current_price) / risk)
    )
    max_extension = _env_float(
        "LTF_V2_MAX_TRIGGER_EXTENSION_R",
        DEFAULT_MAX_TRIGGER_EXTENSION_R,
    )
    if rr < MIN_CONFIRMED_RR:
        outcome = "CONFIRMED_RR_TOO_LOW"
    elif already_moved_r > max_extension:
        outcome = "CONFIRMED_TRIGGER_TOO_EXTENDED"
    else:
        outcome = "CONFIRMED_EXECUTABLE"

    return {
        "outcome": outcome,
        "entry_reference_price": entry,
        "invalidation_reference_price": stop,
        "target_reference_price": target,
        "stop_distance": risk,
        "target_distance": reward,
        "risk_reward_ratio": rr,
        "practical_rr": rr,
        "already_moved_R": already_moved_r,
        "target_source": "interest_zone",
        "target_zone_type": selected.get("zone_type"),
        "target_zone_role": selected.get("zone_role"),
        "target_zone_source": selected.get("source"),
        "target_candidates": candidates,
        "minimum_stop": minimum_stop,
        "stop_buffer": stop_buffer,
        "max_trigger_extension_R": max_extension,
    }


def _apply_geometry(
    setup: dict[str, Any],
    *,
    payload: Mapping[str, Any],
    current_price: float,
    levels: Mapping[str, float],
    at_utc: datetime,
) -> None:
    geometry = _build_geometry(
        setup,
        payload=payload,
        current_price=current_price,
        levels=levels,
    )
    setup["geometry"] = geometry
    setup["geometry_outcome"] = geometry.get("outcome")
    for key in (
        "entry_reference_price",
        "invalidation_reference_price",
        "target_reference_price",
        "stop_distance",
        "target_distance",
        "risk_reward_ratio",
        "practical_rr",
        "already_moved_R",
        "target_source",
        "target_zone_type",
        "target_zone_role",
    ):
        setup[key] = geometry.get(key)

    if geometry.get("outcome") != "CONFIRMED_EXECUTABLE":
        return

    entry_ttl = _env_int(
        "LTF_V2_ENTRY_WINDOW_TTL_MINUTES",
        DEFAULT_ENTRY_WINDOW_TTL_MINUTES,
    )
    setup["entry_ready_at_utc"] = at_utc.isoformat()
    setup["entry_window_expires_at_utc"] = (
        at_utc + timedelta(minutes=max(5, entry_ttl))
    ).isoformat()
    _append_transition(
        setup,
        STATE_ENTRY_READY,
        at_utc=at_utc,
        reason="causal_retest_trigger_and_geometry_confirmed",
        details={
            "entry": geometry.get("entry_reference_price"),
            "stop": geometry.get("invalidation_reference_price"),
            "target": geometry.get("target_reference_price"),
            "rr": geometry.get("risk_reward_ratio"),
        },
    )


def _process_bars(
    setup: dict[str, Any],
    *,
    payload: Mapping[str, Any],
    bars: pd.DataFrame,
    as_of: datetime,
    levels: Mapping[str, float],
) -> None:
    if setup.get("state") in TERMINAL_STATES:
        return

    expiry = _as_utc(setup.get("expires_at_utc"), as_of)
    entry_expiry = _as_utc(setup.get("entry_window_expires_at_utc"))
    if entry_expiry is not None and as_of >= entry_expiry:
        _append_transition(
            setup,
            STATE_EXPIRED,
            at_utc=entry_expiry,
            reason="entry_window_expired",
        )
        setup["terminal_reason"] = "entry_window_expired"
        return
    if expiry is not None and as_of >= expiry:
        _append_transition(
            setup,
            STATE_EXPIRED,
            at_utc=expiry,
            reason="setup_ttl_expired",
        )
        setup["terminal_reason"] = "setup_ttl_expired"
        return

    last_processed = _as_utc(setup.get("last_processed_bar_utc"))
    if last_processed is None:
        last_processed = _as_utc(setup.get("armed_at_utc"), as_of)
    new_indices = [
        index
        for index, value in enumerate(bars["bar_close_utc"])
        if _as_utc(value) is not None and _as_utc(value) > last_processed
    ]

    for index in new_indices:
        row = bars.iloc[index]
        bar_close = _as_utc(row["bar_close_utc"], as_of)
        if bar_close is None:
            continue
        if expiry is not None and bar_close > expiry:
            _append_transition(
                setup,
                STATE_EXPIRED,
                at_utc=expiry,
                reason="setup_ttl_expired",
            )
            setup["terminal_reason"] = "setup_ttl_expired"
            break

        history = bars.iloc[: index + 1]
        previous = bars.iloc[:index]
        atr_value = _atr(history)
        setup["last_processed_bar_utc"] = bar_close.isoformat()
        setup["last_bar"] = _bar_snapshot(row)
        setup["current_price"] = float(row["close"])

        state = str(setup.get("state") or STATE_ARMED)
        if state in TERMINAL_STATES:
            break

        if state == STATE_ENTRY_READY:
            stop = _float(setup.get("invalidation_reference_price"))
            if stop is not None:
                invalidated = (
                    float(row["low"]) <= stop
                    if setup["direction"] == "LONG"
                    else float(row["high"]) >= stop
                )
                if invalidated:
                    _append_transition(
                        setup,
                        STATE_INVALIDATED,
                        at_utc=bar_close,
                        reason="context_invalidation_after_ready",
                        details={"stop": stop, "bar": _bar_snapshot(row)},
                    )
                    setup["terminal_reason"] = "context_invalidation_after_ready"
                    break
            continue

        if state in {
            STATE_RETEST_PENDING,
            STATE_RETEST_TOUCHED,
            STATE_RETEST_HELD,
            STATE_TRIGGER_CONFIRMED,
        } and _is_context_invalidated(setup, row, atr_value=atr_value):
            _append_transition(
                setup,
                STATE_INVALIDATED,
                at_utc=bar_close,
                reason="context_invalidation_before_entry",
                details={"bar": _bar_snapshot(row)},
            )
            setup["terminal_reason"] = "context_invalidation_before_entry"
            break

        if state == STATE_ARMED:
            bos = _detect_bos(setup=setup, row=row, history=previous)
            if bos is None:
                continue
            _append_transition(
                setup,
                STATE_IMPULSE_DETECTED,
                at_utc=bar_close,
                reason="directional_m5_displacement_detected",
                details={"bar": bos["bar"]},
            )
            setup["bos_at_utc"] = bar_close.isoformat()
            setup["bos_level"] = bos["bos_level"]
            setup["impulse_extreme"] = bos["impulse_extreme"]
            setup["atr_at_bos"] = bos["atr"]
            setup["retest_zone_low"] = bos["zone_low"]
            setup["retest_zone_high"] = bos["zone_high"]
            _append_transition(
                setup,
                STATE_BOS_CONFIRMED,
                at_utc=bar_close,
                reason="m5_close_beyond_prior_structure",
                details={
                    "bos_level": bos["bos_level"],
                    "break_buffer": bos["break_buffer"],
                },
            )
            _append_transition(
                setup,
                STATE_RETEST_PENDING,
                at_utc=bar_close,
                reason="bos_confirmed_wait_for_post_bos_retest",
                details={
                    "zone_low": bos["zone_low"],
                    "zone_high": bos["zone_high"],
                },
            )
            continue

        bos_at = _as_utc(setup.get("bos_at_utc"))
        if bos_at is None or bar_close <= bos_at:
            continue

        if state in {STATE_RETEST_PENDING, STATE_RETEST_TOUCHED}:
            if not _touches_retest_zone(setup, row):
                bos_level = _float(setup.get("bos_level"))
                atr_at_bos = _float(setup.get("atr_at_bos")) or atr_value
                if bos_level is not None and atr_at_bos > 0:
                    extension = (
                        float(row["close"]) - bos_level
                        if setup["direction"] == "LONG"
                        else bos_level - float(row["close"])
                    )
                    if extension > atr_at_bos * 3.0:
                        _append_transition(
                            setup,
                            STATE_MISSED,
                            at_utc=bar_close,
                            reason="impulse_extended_without_retest",
                            details={"extension_atr": extension / atr_at_bos},
                        )
                        setup["terminal_reason"] = "impulse_extended_without_retest"
                        break
                continue

            if setup["direction"] == "LONG":
                extreme = float(row["low"])
                previous_extreme = _float(setup.get("retest_extreme"))
                setup["retest_extreme"] = (
                    extreme
                    if previous_extreme is None
                    else min(previous_extreme, extreme)
                )
            else:
                extreme = float(row["high"])
                previous_extreme = _float(setup.get("retest_extreme"))
                setup["retest_extreme"] = (
                    extreme
                    if previous_extreme is None
                    else max(previous_extreme, extreme)
                )

            setup["retest_at_utc"] = bar_close.isoformat()
            setup["retest_bar_high"] = float(row["high"])
            setup["retest_bar_low"] = float(row["low"])
            _append_transition(
                setup,
                STATE_RETEST_TOUCHED,
                at_utc=bar_close,
                reason="post_bos_retest_zone_touched",
                details={"bar": _bar_snapshot(row)},
            )
            if _retest_holds(setup, row):
                _append_transition(
                    setup,
                    STATE_RETEST_HELD,
                    at_utc=bar_close,
                    reason="retest_closed_on_directional_side",
                    details={"bar": _bar_snapshot(row)},
                )
            continue

        if state == STATE_RETEST_HELD:
            retest_at = _as_utc(setup.get("retest_at_utc"))
            if retest_at is None or bar_close <= retest_at:
                continue
            if _trigger_confirms(setup, row, history):
                setup["trigger_at_utc"] = bar_close.isoformat()
                setup["trigger_price"] = float(row["close"])
                _append_transition(
                    setup,
                    STATE_TRIGGER_CONFIRMED,
                    at_utc=bar_close,
                    reason="post_retest_micro_bos_trigger",
                    details={"bar": _bar_snapshot(row)},
                )
                _apply_geometry(
                    setup,
                    payload=payload,
                    current_price=float(row["close"]),
                    levels=levels,
                    at_utc=bar_close,
                )
            elif _touches_retest_zone(setup, row):
                if setup["direction"] == "LONG":
                    setup["retest_extreme"] = min(
                        _float(setup.get("retest_extreme")) or float(row["low"]),
                        float(row["low"]),
                    )
                else:
                    setup["retest_extreme"] = max(
                        _float(setup.get("retest_extreme")) or float(row["high"]),
                        float(row["high"]),
                    )
            continue

        if state == STATE_TRIGGER_CONFIRMED:
            _apply_geometry(
                setup,
                payload=payload,
                current_price=float(row["close"]),
                levels=levels,
                at_utc=bar_close,
            )

    if (
        setup.get("state") == STATE_TRIGGER_CONFIRMED
        and not new_indices
        and setup.get("current_price") is not None
    ):
        _apply_geometry(
            setup,
            payload=payload,
            current_price=float(setup["current_price"]),
            levels=levels,
            at_utc=as_of,
        )


def _pending_outcome_for_state(state: str) -> str:
    return {
        STATE_ARMED: "PENDING_WAITING_FOR_DIRECTIONAL_IMPULSE",
        STATE_IMPULSE_DETECTED: "PENDING_WAITING_FOR_BOS_CONFIRMATION",
        STATE_BOS_CONFIRMED: "PENDING_WAITING_FOR_POST_BOS_RETEST",
        STATE_RETEST_PENDING: "PENDING_WAITING_FOR_POST_BOS_RETEST",
        STATE_RETEST_TOUCHED: "PENDING_RETEST_TOUCHED_WAITING_FOR_HOLD",
        STATE_RETEST_HELD: "PENDING_RETEST_HELD_WAITING_FOR_TRIGGER",
        STATE_TRIGGER_CONFIRMED: "CONFIRMED_PENDING_EXECUTION_FILTERS",
        STATE_ENTRY_READY: "CONFIRMED_EXECUTABLE",
        STATE_INVALIDATED: "REJECTED_CONTEXT_INVALIDATED",
        STATE_EXPIRED: "REJECTED_SETUP_EXPIRED",
        STATE_MISSED: "REJECTED_ENTRY_WINDOW_MISSED",
        STATE_CONTEXT_CANCELLED: "REJECTED_TPO_CONTEXT_CANCELLED",
    }.get(state, "PENDING_UNKNOWN_STATE")


def _next_event_for_state(state: str) -> str:
    return {
        STATE_ARMED: "directional_m5_impulse_and_bos",
        STATE_IMPULSE_DETECTED: "m5_bos_close",
        STATE_BOS_CONFIRMED: "post_bos_retest",
        STATE_RETEST_PENDING: "post_bos_retest",
        STATE_RETEST_TOUCHED: "retest_hold_close",
        STATE_RETEST_HELD: "post_retest_micro_bos_trigger",
        STATE_TRIGGER_CONFIRMED: "valid_real_target_rr_and_freshness",
        STATE_ENTRY_READY: "limit_entry_fill_or_expiry",
    }.get(state, "new_tpo_watch")


def _result_from_setup(
    setup: Mapping[str, Any],
    *,
    active_watch: bool,
    watch_reason: str,
    m5_available: bool,
) -> dict[str, Any]:
    state = str(setup.get("state") or STATE_ARMED)
    family = str(setup.get("setup_family") or "OPEN_TEST_DRIVE")
    profile = SETUP_PROFILES.get(family, SETUP_PROFILES["OPEN_TEST_DRIVE"])
    direction = str(setup.get("direction") or "")
    ready = state == STATE_ENTRY_READY
    rejected = state in TERMINAL_STATES
    compact_state = "CONFIRMED" if ready else ("REJECTED" if rejected else "PENDING")
    full_state = {
        "CONFIRMED": "LTF_MODEL_CONFIRMED",
        "REJECTED": "LTF_MODEL_REJECTED",
        "PENDING": "LTF_MODEL_PENDING",
    }[compact_state]
    outcome = (
        str(setup.get("geometry_outcome"))
        if state == STATE_TRIGGER_CONFIRMED and setup.get("geometry_outcome")
        else _pending_outcome_for_state(state)
    )
    scenario = (
        f"{profile['scenario_prefix']}_{direction}"
        if direction in {"LONG", "SHORT"}
        else f"{profile['scenario_prefix']}_WATCH"
    )
    confidence = float(profile["confidence"]) if ready else min(
        float(profile["confidence"]) - 0.08,
        0.60,
    )
    blockers: list[str] = []
    if not m5_available:
        blockers.append("PENDING_M5_DATA_UNAVAILABLE")
        outcome = "PENDING_M5_DATA_UNAVAILABLE"
    if setup.get("geometry_outcome") and setup.get("geometry_outcome") != "CONFIRMED_EXECUTABLE":
        blockers.append(str(setup["geometry_outcome"]))
    if rejected and setup.get("terminal_reason"):
        blockers.append(str(setup["terminal_reason"]).upper())

    retest_confirmed = state in {
        STATE_RETEST_HELD,
        STATE_TRIGGER_CONFIRMED,
        STATE_ENTRY_READY,
    }
    trigger_confirmed = state in {STATE_TRIGGER_CONFIRMED, STATE_ENTRY_READY}
    result = {
        "ltf_execution_state_machine_version": LTF_EXECUTION_STATE_MACHINE_VERSION,
        "ltf_execution_v2_authoritative": True,
        "ltf_execution_v2_setup_id": setup.get("setup_id"),
        "ltf_execution_v2_state": state,
        "ltf_execution_v2_active_watch": active_watch,
        "ltf_execution_v2_watch_reason": watch_reason,
        "ltf_execution_v2_m5_available": m5_available,
        "ltf_execution_v2_armed_at_utc": setup.get("armed_at_utc"),
        "ltf_execution_v2_updated_at_utc": setup.get("updated_at_utc"),
        "ltf_execution_v2_expires_at_utc": setup.get("expires_at_utc"),
        "ltf_execution_v2_last_processed_bar_utc": setup.get(
            "last_processed_bar_utc"
        ),
        "ltf_execution_v2_transition_history": deepcopy(
            setup.get("transition_history") or []
        ),
        "ltf_execution_v2_reference_level": setup.get("reference_level"),
        "ltf_execution_v2_reference_level_label": setup.get(
            "reference_level_label"
        ),
        "ltf_execution_v2_bos_at_utc": setup.get("bos_at_utc"),
        "ltf_execution_v2_bos_level": setup.get("bos_level"),
        "ltf_execution_v2_retest_at_utc": setup.get("retest_at_utc"),
        "ltf_execution_v2_trigger_at_utc": setup.get("trigger_at_utc"),
        "ltf_execution_v2_entry_ready_at_utc": setup.get("entry_ready_at_utc"),
        "entry_window_started_at_utc": setup.get("entry_ready_at_utc"),
        "entry_window_expires_at_utc": setup.get(
            "entry_window_expires_at_utc"
        ),
        "ltf_model_state": compact_state,
        "ltf_model_state_full": full_state,
        "ltf_model_outcome": outcome,
        "ltf_model_type": profile["model"],
        "ltf_model_confirmed": ready,
        "auction_ltf_setup": family,
        "direction": direction or None,
        "decision": (
            "TRADEABLE"
            if ready
            else ("NO_TRADE" if rejected else "WATCH")
        ),
        "status": "READY" if ready else "WATCH",
        "signal_class": "READY" if ready else "WATCH",
        "stage": "READY" if ready else "WATCH",
        "setup_type": f"TPO_{family}",
        "scenario": scenario,
        "scenario_type": scenario,
        "execution_status": "EXECUTABLE" if ready else "NOT_EXECUTABLE",
        "execution_model": profile["model"] if ready else "NONE",
        "execution_timeframe": "5m",
        "trigger_reason": (
            f"ltf_v2_confirmed_{family.lower()}"
            if ready
            else _next_event_for_state(state)
        ),
        "next_expected_event": _next_event_for_state(state),
        "entry_reference_price": setup.get("entry_reference_price"),
        "invalidation_reference_price": setup.get(
            "invalidation_reference_price"
        ),
        "target_reference_price": setup.get("target_reference_price"),
        "stop_distance": setup.get("stop_distance"),
        "target_distance": setup.get("target_distance"),
        "risk_reward_ratio": setup.get("risk_reward_ratio"),
        "practical_rr": setup.get("practical_rr"),
        "target_source": setup.get("target_source"),
        "target_zone_type": setup.get("target_zone_type"),
        "target_zone_role": setup.get("target_zone_role"),
        "current_price": setup.get("current_price"),
        "already_moved_R": (
            (setup.get("geometry") or {}).get("already_moved_R")
            if isinstance(setup.get("geometry"), Mapping)
            else None
        ),
        "stop_quality": "OK" if ready else None,
        "fresh_retest_exists": retest_confirmed,
        "fresh_failed_acceptance_exists": bool(
            retest_confirmed
            and family
            in {"OPEN_TEST_DRIVE", "OPEN_AUCTION_BACK_TO_VALUE"}
        ),
        "fresh_pullback_exists": bool(
            retest_confirmed
            and family in {"OPEN_DRIVE", "OPEN_AUCTION_BREAKOUT"}
        ),
        "retest_confirmed": retest_confirmed,
        "acceptance_confirmed": retest_confirmed,
        "ltf_confirmed": ready,
        "fresh_entry_window": ready,
        "ltf_entry_window_detected": ready,
        "entry_window_type": (
            "FAILED_ACCEPTANCE_RETEST"
            if family in {"OPEN_TEST_DRIVE", "OPEN_AUCTION_BACK_TO_VALUE"}
            else "CONTINUATION_RETEST"
        ),
        "entry_window_state": (
            "CONFIRMED"
            if ready
            else (
                "PENDING_RETEST"
                if state
                in {
                    STATE_BOS_CONFIRMED,
                    STATE_RETEST_PENDING,
                    STATE_RETEST_TOUCHED,
                }
                else state
            )
        ),
        "entry_window_direction": direction or None,
        "entry_window_quality": "GOOD" if ready else "PENDING",
        "entry_window_confidence": confidence,
        "confidence": confidence,
        "probability": confidence,
        "ltf_entry_model_hint": profile["model"],
        "entry_model_hint": profile["model"],
        "stop_model_hint": "BEYOND_CONTEXT_INVALIDATION_ZONE",
        "target_model_hint": "NEXT_REAL_INTEREST_ZONE",
        "stop_anchor": "RETEST_OR_CONTEXT_INVALIDATION_EXTREME",
        "context_invalidation_price": setup.get(
            "context_invalidation_price"
        ),
        "context_stop_price": setup.get("invalidation_reference_price"),
        "context_stop_valid": ready,
        "context_stop_reason": (
            "stop_beyond_retest_and_context_invalidation"
            if ready
            else "waiting_for_complete_retest_geometry"
        ),
        "ltf_model_blockers": blockers,
        "ltf_entry_window_blockers": blockers,
        "ltf_model_reasons": [
            f"Authoritative causal state: {state}.",
            "BOS and retest are timestamp-ordered; a BOS never implies a fresh retest.",
        ],
        "ltf_model_warnings": (
            ["auction_setup_requires_caution"]
            if profile.get("requires_caution")
            else []
        ),
        "execution": {
            "status": "EXECUTABLE" if ready else "NOT_EXECUTABLE",
            "model": profile["model"] if ready else "NONE",
            "entry_reference_price": setup.get("entry_reference_price"),
            "invalidation_reference_price": setup.get(
                "invalidation_reference_price"
            ),
            "target_reference_price": setup.get("target_reference_price"),
            "risk_reward_ratio": setup.get("risk_reward_ratio"),
            "stop_distance": setup.get("stop_distance"),
            "target_distance": setup.get("target_distance"),
            "execution_timeframe": "5m",
            "trigger_reason": (
                f"ltf_v2_confirmed_{family.lower()}"
                if ready
                else _next_event_for_state(state)
            ),
            "ltf_execution_v2_setup_id": setup.get("setup_id"),
            "ltf_execution_v2_state": state,
        },
        "ltf_execution_v2_diagnostics": {
            "setup": deepcopy(dict(setup)),
            "geometry": deepcopy(setup.get("geometry") or {}),
        },
    }
    return result


def _no_active_result(reason: str) -> dict[str, Any]:
    return {
        "ltf_execution_state_machine_version": LTF_EXECUTION_STATE_MACHINE_VERSION,
        "ltf_execution_v2_authoritative": True,
        "ltf_execution_v2_setup_id": None,
        "ltf_execution_v2_state": "NO_ACTIVE_WATCH",
        "ltf_execution_v2_active_watch": False,
        "ltf_execution_v2_watch_reason": reason,
        "ltf_execution_v2_m5_available": False,
        "ltf_model_state": "NO_MODEL",
        "ltf_model_state_full": "LTF_MODEL_NO_MODEL",
        "ltf_model_outcome": "NO_ACTIVE_TPO_AUCTION_WATCH",
        "ltf_model_confirmed": False,
        "execution_status": "NOT_EXECUTABLE",
        "execution_model": "NONE",
        "fresh_retest_exists": False,
        "retest_confirmed": False,
        "acceptance_confirmed": False,
        "ltf_confirmed": False,
        "fresh_entry_window": False,
        "ltf_entry_window_detected": False,
        "ltf_model_blockers": [reason],
    }


@dataclass(slots=True)
class LTFExecutionStateStore:
    """Small atomic JSON state store used by the sequential production worker."""

    path: Path | None = None
    terminal_retention_days: int = DEFAULT_TERMINAL_RETENTION_DAYS
    _document: dict[str, Any] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._document = self._load()

    def _empty_document(self) -> dict[str, Any]:
        return {
            "schema_version": LTF_EXECUTION_STATE_SCHEMA_VERSION,
            "engine_version": LTF_EXECUTION_STATE_MACHINE_VERSION,
            "updated_at_utc": None,
            "active_by_symbol": {},
            "setups": {},
        }

    def _load(self) -> dict[str, Any]:
        if self.path is None or not self.path.exists():
            return self._empty_document()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return self._empty_document()
        if not isinstance(data, dict):
            return self._empty_document()
        if not isinstance(data.get("setups"), dict):
            data["setups"] = {}
        if not isinstance(data.get("active_by_symbol"), dict):
            data["active_by_symbol"] = {}
        data["schema_version"] = LTF_EXECUTION_STATE_SCHEMA_VERSION
        data["engine_version"] = LTF_EXECUTION_STATE_MACHINE_VERSION
        return data

    def _save(self) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        payload = json.dumps(
            self._document,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        temporary.write_text(payload + "\n", encoding="utf-8")
        temporary.replace(self.path)

    def _prune(self, now: datetime) -> None:
        cutoff = now - timedelta(days=max(1, self.terminal_retention_days))
        setups = self._document["setups"]
        active_ids = set(self._document["active_by_symbol"].values())
        remove: list[str] = []
        for setup_id, setup in setups.items():
            if setup_id in active_ids or not isinstance(setup, Mapping):
                continue
            if str(setup.get("state")) not in TERMINAL_STATES:
                continue
            updated = _as_utc(setup.get("updated_at_utc"))
            if updated is not None and updated < cutoff:
                remove.append(setup_id)
        for setup_id in remove:
            setups.pop(setup_id, None)

    def evaluate(
        self,
        payload: Mapping[str, Any],
        *,
        df_5m: pd.DataFrame | None,
        df_15m: pd.DataFrame | None = None,
        as_of: datetime | str | None = None,
        persist: bool = True,
    ) -> dict[str, Any]:
        del df_15m  # reserved for a future structural target audit
        now = _as_utc(as_of, datetime.now(UTC)) or datetime.now(UTC)
        active, watch_reason = is_active_tpo_watch(payload)
        symbol = _symbol(payload)
        current_active_id = self._document["active_by_symbol"].get(symbol)

        if not active:
            if current_active_id:
                existing = self._document["setups"].get(current_active_id)
                if isinstance(existing, dict) and existing.get("state") not in TERMINAL_STATES:
                    _append_transition(
                        existing,
                        STATE_CONTEXT_CANCELLED,
                        at_utc=now,
                        reason=watch_reason,
                    )
                    existing["terminal_reason"] = watch_reason
                self._document["active_by_symbol"].pop(symbol, None)
            self._document["updated_at_utc"] = now.isoformat()
            self._prune(now)
            if persist:
                self._save()
            return _no_active_result(watch_reason)

        family = _canonical_setup_family(payload)
        direction = _direction(payload, family)
        if family is None or direction not in {"LONG", "SHORT"}:
            result = _no_active_result(
                "active_watch_missing_supported_family_or_direction"
            )
            result.update(
                {
                    "ltf_execution_v2_active_watch": True,
                    "ltf_execution_v2_watch_reason": watch_reason,
                    "ltf_execution_v2_state": STATE_ARMED,
                    "ltf_model_state": "PENDING",
                    "ltf_model_state_full": "LTF_MODEL_PENDING",
                    "ltf_model_outcome": "PENDING_MISSING_DIRECTION",
                    "ltf_model_blockers": ["PENDING_MISSING_DIRECTION"],
                }
            )
            return result

        levels = _collect_levels(payload)
        reference_level, reference_label = _reference_level(
            payload,
            setup_family=family,
            direction=direction,
            levels=levels,
        )
        reference_profile_id = _raw_text(
            _first_value(payload, "reference_profile_id")
        )
        anchor = _setup_anchor(payload, now)
        resolved_setup_id = _setup_id(
            symbol=symbol,
            direction=direction,
            setup_family=family,
            anchor=anchor,
            reference_profile_id=reference_profile_id,
        )

        if current_active_id and current_active_id != resolved_setup_id:
            previous = self._document["setups"].get(current_active_id)
            if isinstance(previous, dict) and previous.get("state") not in TERMINAL_STATES:
                _append_transition(
                    previous,
                    STATE_CONTEXT_CANCELLED,
                    at_utc=now,
                    reason="tpo_watch_context_replaced",
                )
                previous["terminal_reason"] = "tpo_watch_context_replaced"

        setup = self._document["setups"].get(resolved_setup_id)
        if not isinstance(setup, dict):
            setup = _create_setup(
                payload,
                as_of=now,
                symbol=symbol,
                direction=direction,
                setup_family=family,
                setup_id=resolved_setup_id,
                levels=levels,
                reference_level=reference_level,
                reference_label=reference_label,
            )
            self._document["setups"][resolved_setup_id] = setup

        if setup.get("state") not in TERMINAL_STATES:
            self._document["active_by_symbol"][symbol] = resolved_setup_id
        bars = _normalize_bars(df_5m, as_of=now)
        m5_available = len(bars) >= _env_int(
            "LTF_V2_MIN_HISTORY_BARS",
            DEFAULT_MIN_HISTORY_BARS,
        )
        if m5_available:
            _process_bars(
                setup,
                payload=payload,
                bars=bars,
                as_of=now,
                levels=levels,
            )
        else:
            setup["updated_at_utc"] = now.isoformat()
            setup["m5_rows_available"] = int(len(bars))

        if setup.get("state") in TERMINAL_STATES:
            self._document["active_by_symbol"].pop(symbol, None)
        self._document["updated_at_utc"] = now.isoformat()
        self._prune(now)
        if persist:
            self._save()
        return _result_from_setup(
            setup,
            active_watch=True,
            watch_reason=watch_reason,
            m5_available=m5_available,
        )

    def snapshot(self) -> dict[str, Any]:
        return deepcopy(self._document)


def enrich_payload_with_ltf_execution_v2(
    payload: Mapping[str, Any],
    *,
    df_5m: pd.DataFrame | None,
    df_15m: pd.DataFrame | None = None,
    store: LTFExecutionStateStore | None = None,
    state_path: Path | str | None = None,
    as_of: datetime | str | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    """Return a shallow payload copy enriched by the authoritative v2 result."""

    enriched = dict(payload or {})
    effective_store = store or LTFExecutionStateStore(
        Path(state_path) if state_path is not None else None
    )
    result = effective_store.evaluate(
        enriched,
        df_5m=df_5m,
        df_15m=df_15m,
        as_of=as_of,
        persist=persist,
    )
    enriched.update(result)

    metadata = enriched.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    metadata.update(result)
    metadata["ltf_execution_v2"] = result
    enriched["metadata"] = metadata
    return enriched


__all__ = [
    "LTF_EXECUTION_STATE_MACHINE_VERSION",
    "LTFExecutionStateStore",
    "STATE_ARMED",
    "STATE_BOS_CONFIRMED",
    "STATE_CONTEXT_CANCELLED",
    "STATE_ENTRY_READY",
    "STATE_EXPIRED",
    "STATE_IMPULSE_DETECTED",
    "STATE_INVALIDATED",
    "STATE_MISSED",
    "STATE_RETEST_HELD",
    "STATE_RETEST_PENDING",
    "STATE_RETEST_TOUCHED",
    "STATE_TRIGGER_CONFIRMED",
    "enrich_payload_with_ltf_execution_v2",
    "is_active_tpo_watch",
]
