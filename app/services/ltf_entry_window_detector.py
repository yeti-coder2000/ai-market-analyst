"""LTF entry-window detector for AI Market Analyst.

Version: ltf-entry-window-detector-v1.0-auction-execution-layer

Purpose
-------
This module does not create a trade signal by itself. It enriches an existing
auction/TPO/HTF payload with execution-layer information:

- Is there a fresh LTF entry window?
- Is the market only chasing an already-delivered impulse?
- Which execution model is active: sweep-reclaim, continuation retest,
  or failed-acceptance retest?
- Which fields should downstream runner/Battle Gate use to allow or suppress
  Telegram READY?

The detector is intentionally conservative in v1. It prefers PENDING/FORMING
instead of CONFIRMED when price structure is incomplete or the payload lacks
recent candles.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


LTF_ENTRY_WINDOW_DETECTOR_VERSION = "ltf-entry-window-detector-v1.0-auction-execution-layer"

# Entry window types
ENTRY_WINDOW_NONE = "ENTRY_WINDOW_NONE"
ENTRY_WINDOW_SWEEP_RECLAIM_RETEST = "ENTRY_WINDOW_SWEEP_RECLAIM_RETEST"
ENTRY_WINDOW_CONTINUATION_RETEST = "ENTRY_WINDOW_CONTINUATION_RETEST"
ENTRY_WINDOW_FAILED_ACCEPTANCE_RETEST = "ENTRY_WINDOW_FAILED_ACCEPTANCE_RETEST"
ENTRY_WINDOW_LATE_CHASE = "ENTRY_WINDOW_LATE_CHASE"
ENTRY_WINDOW_PULLBACK_PENDING = "ENTRY_WINDOW_PULLBACK_PENDING"
ENTRY_WINDOW_RETEST_PENDING = "ENTRY_WINDOW_RETEST_PENDING"
ENTRY_WINDOW_CONFIRMED = "ENTRY_WINDOW_CONFIRMED"
ENTRY_WINDOW_EXHAUSTED = "ENTRY_WINDOW_EXHAUSTED"

# Entry window states
STATE_NONE = "NONE"
STATE_FORMING = "FORMING"
STATE_PENDING_RETEST = "PENDING_RETEST"
STATE_CONFIRMED = "CONFIRMED"
STATE_LATE = "LATE"
STATE_INVALID = "INVALID"

# Entry quality labels
ENTRY_QUALITY_GOOD = "GOOD"
ENTRY_QUALITY_WEAK = "WEAK"
ENTRY_QUALITY_LATE = "LATE"
ENTRY_QUALITY_INVALID = "INVALID"
ENTRY_QUALITY_UNKNOWN = "UNKNOWN"

# Model hints
MODEL_SWEEP_RECLAIM_BOS_RETEST = "SWEEP_RECLAIM_BOS_RETEST"
MODEL_CONTINUATION_PULLBACK_RETEST = "CONTINUATION_PULLBACK_RETEST"
MODEL_FAILED_ACCEPTANCE_RETEST = "FAILED_ACCEPTANCE_RETEST"
MODEL_NO_CLEAN_ENTRY = "NO_CLEAN_ENTRY"

STOP_BEHIND_SWEEP_EXTREME = "BEHIND_SWEEP_EXTREME"
STOP_BEHIND_PULLBACK_STRUCTURE = "BEHIND_PULLBACK_STRUCTURE"
STOP_BEHIND_FAILED_ACCEPTANCE_EXTREME = "BEHIND_FAILED_ACCEPTANCE_EXTREME"
STOP_UNDEFINED = "UNDEFINED"

TARGET_TO_VALUE_EDGE_OR_LIQUIDITY = "TO_VALUE_EDGE_OR_LIQUIDITY"
TARGET_TO_NEXT_INTEREST_ZONE = "TO_NEXT_INTEREST_ZONE"
TARGET_UNDEFINED = "UNDEFINED"

# Conservative defaults. They can be overridden by fields/env at the runner level later.
DEFAULT_MAX_CONFIRMED_ALREADY_MOVED_R = 0.75
DEFAULT_MAX_PENDING_ALREADY_MOVED_R = 1.15
DEFAULT_MIN_DISPLACEMENT_R = 0.25
DEFAULT_MIN_CONFIRMATION_SCORE = 0.62
DEFAULT_SYMBOL_TOLERANCE_PCT = 0.0015  # 0.15%; used only when no instrument tick/ATR is provided.


@dataclass
class Candle:
    """Minimal normalized OHLC candle."""

    open: float
    high: float
    low: float
    close: float
    timestamp: Optional[str] = None
    volume: Optional[float] = None


@dataclass
class LtfEntryWindowResult:
    """Stable result schema for downstream runner, Battle Gate, telemetry, and briefing."""

    detector_version: str = LTF_ENTRY_WINDOW_DETECTOR_VERSION
    ltf_entry_window_detected: bool = False
    entry_window_type: str = ENTRY_WINDOW_NONE
    entry_window_state: str = STATE_NONE
    entry_window_direction: Optional[str] = None

    fresh_entry_window: bool = False
    retest_confirmed: bool = False
    acceptance_confirmed: bool = False
    ltf_confirmed: bool = False
    continuation_ready_after_retest: bool = False

    late_entry_risk: bool = False
    already_moved_R: Optional[float] = None
    entry_quality: str = ENTRY_QUALITY_UNKNOWN
    confidence: float = 0.0

    entry_model_hint: str = MODEL_NO_CLEAN_ENTRY
    stop_model_hint: str = STOP_UNDEFINED
    target_model_hint: str = TARGET_UNDEFINED

    liquidity_level: Optional[float] = None
    reclaim_level: Optional[float] = None
    retest_level: Optional[float] = None
    sweep_extreme: Optional[float] = None
    pullback_extreme: Optional[float] = None

    blockers: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    relevant_levels: Dict[str, Any] = field(default_factory=dict)
    debug: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        # Downstream expects simple JSON-friendly values.
        data["confidence"] = round(float(data.get("confidence") or 0.0), 4)
        if data.get("already_moved_R") is not None:
            data["already_moved_R"] = round(float(data["already_moved_R"]), 4)
        return data


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_ltf_entry_window(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Detect whether the payload contains a fresh 5m-15m entry window.

    The function accepts a flexible payload because upstream services in this
    project use slightly different field names. It is safe to call even with
    sparse payloads; in that case it returns ENTRY_WINDOW_NONE or PENDING rather
    than throwing.
    """

    p: Dict[str, Any] = dict(payload or {})
    direction = _infer_direction(p)
    result = LtfEntryWindowResult(entry_window_direction=direction)

    candles = _extract_ltf_candles(p)
    levels = _collect_relevant_levels(p)
    result.relevant_levels = dict(levels)

    entry = _first_float(p, "entry_reference_price", "entry", "entry_price", "planned_entry")
    stop = _first_float(
        p,
        "invalidation_reference_price",
        "stop_loss",
        "stop",
        "sl",
        "planned_stop",
    )
    current = _first_float(p, "current_price", "last_price", "price", "close")
    result.already_moved_R = _compute_already_moved_r(direction, entry, stop, current, p)

    # 1) Hard late-chase classification first. Later models may override only
    # if they can prove a fresh retest/reclaim window.
    late_result = _detect_late_chase(p, direction, result.already_moved_R)

    # 2) Primary execution models.
    sweep = _detect_sweep_reclaim_retest(p, direction, candles, levels, result.already_moved_R)
    cont = _detect_continuation_retest(p, direction, candles, levels, result.already_moved_R)
    failed = _detect_failed_acceptance_retest(p, direction, candles, levels, result.already_moved_R)

    candidates = [r for r in (sweep, cont, failed) if r is not None]

    if not candidates:
        if late_result is not None:
            return late_result.to_dict()
        result.blockers.append("no_ltf_entry_window_detected")
        result.reasons.append("No sweep/reclaim, continuation retest, or failed-acceptance retest was confirmed.")
        result.debug.update(_base_debug(p, candles, direction, levels))
        return result.to_dict()

    best = _choose_best_candidate(candidates)

    # If a model is confirmed/forming, it may intentionally override late-chase.
    # If it is too weak/pending and late-risk exists, keep late state as blocker.
    if late_result is not None and not best.fresh_entry_window:
        best.late_entry_risk = True
        best.blockers.append("late_chase_without_fresh_retest")
        best.warnings.extend(late_result.warnings)
        best.reasons.append("Late impulse risk remains because no fresh retest entry was confirmed.")
        if best.entry_window_state not in {STATE_CONFIRMED, STATE_FORMING}:
            best.entry_window_type = ENTRY_WINDOW_LATE_CHASE
            best.entry_window_state = STATE_LATE
            best.entry_quality = ENTRY_QUALITY_LATE
            best.fresh_entry_window = False
            best.ltf_entry_window_detected = False

    best.debug.update(_base_debug(p, candles, direction, levels))
    return best.to_dict()


# Backward-compatible aliases for possible runner integration names.
def detect(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return detect_ltf_entry_window(payload)


def evaluate_ltf_entry_window(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return detect_ltf_entry_window(payload)


# ---------------------------------------------------------------------------
# Detection models
# ---------------------------------------------------------------------------


def _detect_sweep_reclaim_retest(
    payload: Mapping[str, Any],
    direction: str,
    candles: Sequence[Candle],
    levels: Mapping[str, Any],
    already_moved_r: Optional[float],
) -> Optional[LtfEntryWindowResult]:
    if direction not in {"LONG", "SHORT"}:
        return None

    haystack = _haystack(payload)
    token_score = _token_score(
        haystack,
        (
            "SWEEP_RECLAIM",
            "LIQUIDITY_SWEEP",
            "FAILED_BREAKDOWN_RECLAIM",
            "FAILED_BREAKOUT_RECLAIM",
            "RECLAIM",
            "SWEEP",
            "BOS_RETEST",
        ),
    )

    liquidity_level = _choose_liquidity_level(direction, levels)
    candle_signal = _candle_sweep_reclaim_signal(direction, candles, liquidity_level, payload)

    # Accept upstream explicit confirmation even when candles are absent.
    upstream_retest = _any_bool(payload, "retest_confirmed", "sweep_retest_confirmed", "ltf_retest_confirmed")
    upstream_acceptance = _any_bool(payload, "acceptance_confirmed", "reclaim_confirmed", "sweep_reclaim_confirmed")
    upstream_ltf = _any_bool(payload, "ltf_confirmed", "ltf_model_confirmed", "bos_confirmed", "structure_shift_confirmed")

    score = 0.0
    reasons: List[str] = []
    warnings: List[str] = []
    blockers: List[str] = []

    if token_score:
        score += min(0.25, token_score * 0.05)
        reasons.append("Payload contains sweep/reclaim execution tokens.")
    if candle_signal["sweep"]:
        score += 0.22
        reasons.append("Price swept a nearby liquidity/value level.")
    if candle_signal["reclaim"]:
        score += 0.22
        reasons.append("Price reclaimed back through the swept level.")
    if candle_signal["displacement"] or upstream_ltf:
        score += 0.16
        reasons.append("LTF displacement / structure shift is present.")
    if candle_signal["retest"] or upstream_retest:
        score += 0.18
        reasons.append("Retest of reclaim zone is present.")
    if upstream_acceptance:
        score += 0.10
        reasons.append("Upstream acceptance/reclaim confirmation is present.")

    retest_confirmed = bool(candle_signal["retest"] or upstream_retest)
    acceptance_confirmed = bool(candle_signal["reclaim"] or upstream_acceptance)
    ltf_confirmed = bool(candle_signal["displacement"] or upstream_ltf)

    if liquidity_level is None and not token_score:
        blockers.append("no_liquidity_level_for_sweep_reclaim")
    if score < 0.38:
        return None

    state = STATE_FORMING
    window_type = ENTRY_WINDOW_SWEEP_RECLAIM_RETEST
    quality = ENTRY_QUALITY_WEAK
    detected = True
    fresh = False

    if retest_confirmed and acceptance_confirmed and ltf_confirmed and score >= DEFAULT_MIN_CONFIRMATION_SCORE:
        state = STATE_CONFIRMED
        quality = ENTRY_QUALITY_GOOD
        fresh = True
    elif acceptance_confirmed and ltf_confirmed:
        state = STATE_PENDING_RETEST
        window_type = ENTRY_WINDOW_RETEST_PENDING
        warnings.append("Sweep/reclaim formed, but clean retest is still pending.")
    else:
        state = STATE_FORMING
        warnings.append("Sweep/reclaim context is forming, but confirmation is incomplete.")

    if already_moved_r is not None and already_moved_r > DEFAULT_MAX_PENDING_ALREADY_MOVED_R and not fresh:
        state = STATE_LATE
        window_type = ENTRY_WINDOW_LATE_CHASE
        quality = ENTRY_QUALITY_LATE
        detected = False
        blockers.append("sweep_reclaim_too_late_without_retest")
    elif already_moved_r is not None and already_moved_r > DEFAULT_MAX_CONFIRMED_ALREADY_MOVED_R and fresh:
        warnings.append("Fresh retest exists, but impulse extension is elevated; require strict RR/stop validation.")

    extreme = candle_signal.get("sweep_extreme")
    retest_level = candle_signal.get("retest_level") or liquidity_level

    return LtfEntryWindowResult(
        ltf_entry_window_detected=detected,
        entry_window_type=window_type,
        entry_window_state=state,
        entry_window_direction=direction,
        fresh_entry_window=fresh,
        retest_confirmed=retest_confirmed,
        acceptance_confirmed=acceptance_confirmed,
        ltf_confirmed=ltf_confirmed,
        continuation_ready_after_retest=False,
        late_entry_risk=bool(already_moved_r is not None and already_moved_r > DEFAULT_MAX_CONFIRMED_ALREADY_MOVED_R),
        already_moved_R=already_moved_r,
        entry_quality=quality,
        confidence=min(0.95, score),
        entry_model_hint=MODEL_SWEEP_RECLAIM_BOS_RETEST,
        stop_model_hint=STOP_BEHIND_SWEEP_EXTREME,
        target_model_hint=TARGET_TO_VALUE_EDGE_OR_LIQUIDITY,
        liquidity_level=liquidity_level,
        reclaim_level=liquidity_level,
        retest_level=retest_level,
        sweep_extreme=extreme,
        blockers=blockers,
        warnings=warnings,
        reasons=reasons,
        relevant_levels=dict(levels),
    )


def _detect_continuation_retest(
    payload: Mapping[str, Any],
    direction: str,
    candles: Sequence[Candle],
    levels: Mapping[str, Any],
    already_moved_r: Optional[float],
) -> Optional[LtfEntryWindowResult]:
    if direction not in {"LONG", "SHORT"}:
        return None

    haystack = _haystack(payload)
    continuation_context = any(
        token in haystack
        for token in (
            "TREND_CONTINUATION",
            "CONTINUATION",
            "OPEN_DRIVE",
            "DIRECTIONAL_IMBALANCE",
            "RANGE_EXTENSION",
            "IMPULSE",
        )
    )
    if not continuation_context:
        return None

    breakout_level = _choose_breakout_or_base_level(direction, levels, payload)
    candle_signal = _candle_continuation_retest_signal(direction, candles, breakout_level, payload)

    upstream_retest = _any_bool(payload, "retest_confirmed", "pullback_retest_confirmed", "breakout_retest_confirmed")
    upstream_acceptance = _any_bool(payload, "acceptance_confirmed", "breakout_acceptance_confirmed", "pullback_holds")
    upstream_ltf = _any_bool(payload, "ltf_confirmed", "ltf_model_confirmed", "continuation_confirmed")

    score = 0.28
    reasons = ["Directional continuation context is present."]
    warnings: List[str] = []
    blockers: List[str] = []

    if candle_signal["pullback"]:
        score += 0.20
        reasons.append("Price pulled back instead of entering only at extension.")
    if candle_signal["retest"] or upstream_retest:
        score += 0.22
        reasons.append("Pullback/retest zone is present.")
    if candle_signal["holds_structure"] or upstream_acceptance:
        score += 0.18
        reasons.append("Pullback held structure / acceptance.")
    if candle_signal["continuation_trigger"] or upstream_ltf:
        score += 0.18
        reasons.append("LTF continuation trigger is present.")

    retest_confirmed = bool(candle_signal["retest"] or upstream_retest)
    acceptance_confirmed = bool(candle_signal["holds_structure"] or upstream_acceptance)
    ltf_confirmed = bool(candle_signal["continuation_trigger"] or upstream_ltf)

    fresh = retest_confirmed and acceptance_confirmed and ltf_confirmed and score >= DEFAULT_MIN_CONFIRMATION_SCORE
    state = STATE_CONFIRMED if fresh else STATE_PENDING_RETEST
    quality = ENTRY_QUALITY_GOOD if fresh else ENTRY_QUALITY_WEAK
    detected = True
    window_type = ENTRY_WINDOW_CONTINUATION_RETEST if fresh else ENTRY_WINDOW_PULLBACK_PENDING

    if already_moved_r is not None and already_moved_r > DEFAULT_MAX_PENDING_ALREADY_MOVED_R and not fresh:
        state = STATE_LATE
        window_type = ENTRY_WINDOW_LATE_CHASE
        quality = ENTRY_QUALITY_LATE
        detected = False
        blockers.append("continuation_too_extended_without_clean_retest")
    elif already_moved_r is not None and already_moved_r > DEFAULT_MAX_CONFIRMED_ALREADY_MOVED_R and fresh:
        warnings.append("Continuation retest is fresh, but extension is elevated; Battle must validate RR and stop strictly.")

    if not retest_confirmed:
        warnings.append("Continuation context exists, but pullback/retest is not confirmed yet.")

    return LtfEntryWindowResult(
        ltf_entry_window_detected=detected,
        entry_window_type=window_type,
        entry_window_state=state,
        entry_window_direction=direction,
        fresh_entry_window=fresh,
        retest_confirmed=retest_confirmed,
        acceptance_confirmed=acceptance_confirmed,
        ltf_confirmed=ltf_confirmed,
        continuation_ready_after_retest=fresh,
        late_entry_risk=bool(already_moved_r is not None and already_moved_r > DEFAULT_MAX_CONFIRMED_ALREADY_MOVED_R),
        already_moved_R=already_moved_r,
        entry_quality=quality,
        confidence=min(0.95, score),
        entry_model_hint=MODEL_CONTINUATION_PULLBACK_RETEST,
        stop_model_hint=STOP_BEHIND_PULLBACK_STRUCTURE,
        target_model_hint=TARGET_TO_NEXT_INTEREST_ZONE,
        retest_level=candle_signal.get("retest_level") or breakout_level,
        pullback_extreme=candle_signal.get("pullback_extreme"),
        blockers=blockers,
        warnings=warnings,
        reasons=reasons,
        relevant_levels=dict(levels),
    )


def _detect_failed_acceptance_retest(
    payload: Mapping[str, Any],
    direction: str,
    candles: Sequence[Candle],
    levels: Mapping[str, Any],
    already_moved_r: Optional[float],
) -> Optional[LtfEntryWindowResult]:
    if direction not in {"LONG", "SHORT"}:
        return None

    haystack = _haystack(payload)
    failed_acceptance_context = any(
        token in haystack
        for token in (
            "FAILED_ACCEPTANCE",
            "FAILED_ACCEPTANCE_RETEST",
            "OPEN_TEST_DRIVE",
            "OTD_RETEST",
            "VALUE_REJECTION",
            "REJECT_VALUE",
        )
    )
    if not failed_acceptance_context:
        return None

    value_edge = _choose_value_edge(direction, levels)
    candle_signal = _candle_failed_acceptance_signal(direction, candles, value_edge, payload)

    upstream_retest = _any_bool(payload, "retest_confirmed", "failed_acceptance_retest_confirmed")
    upstream_acceptance = _any_bool(payload, "acceptance_confirmed", "failed_acceptance_confirmed", "value_rejection_confirmed")
    upstream_ltf = _any_bool(payload, "ltf_confirmed", "ltf_model_confirmed")

    score = 0.30
    reasons = ["Auction context contains failed-acceptance / OTD tokens."]
    warnings: List[str] = []
    blockers: List[str] = []

    if candle_signal["tested_edge"]:
        score += 0.18
        reasons.append("Price tested a value/range edge.")
    if candle_signal["failed_acceptance"] or upstream_acceptance:
        score += 0.22
        reasons.append("Acceptance beyond the edge failed.")
    if candle_signal["retest"] or upstream_retest:
        score += 0.18
        reasons.append("Retest after failed acceptance is present.")
    if candle_signal["ltf_shift"] or upstream_ltf:
        score += 0.16
        reasons.append("LTF structure shifted back in the auction direction.")

    retest_confirmed = bool(candle_signal["retest"] or upstream_retest)
    acceptance_confirmed = bool(candle_signal["failed_acceptance"] or upstream_acceptance)
    ltf_confirmed = bool(candle_signal["ltf_shift"] or upstream_ltf)

    fresh = retest_confirmed and acceptance_confirmed and ltf_confirmed and score >= DEFAULT_MIN_CONFIRMATION_SCORE
    state = STATE_CONFIRMED if fresh else STATE_PENDING_RETEST
    quality = ENTRY_QUALITY_GOOD if fresh else ENTRY_QUALITY_WEAK
    detected = True
    window_type = ENTRY_WINDOW_FAILED_ACCEPTANCE_RETEST if fresh else ENTRY_WINDOW_RETEST_PENDING

    if not retest_confirmed:
        warnings.append("Failed acceptance context exists, but retest is still pending.")
    if already_moved_r is not None and already_moved_r > DEFAULT_MAX_PENDING_ALREADY_MOVED_R and not fresh:
        state = STATE_LATE
        window_type = ENTRY_WINDOW_LATE_CHASE
        quality = ENTRY_QUALITY_LATE
        detected = False
        blockers.append("failed_acceptance_entry_too_late_without_retest")

    return LtfEntryWindowResult(
        ltf_entry_window_detected=detected,
        entry_window_type=window_type,
        entry_window_state=state,
        entry_window_direction=direction,
        fresh_entry_window=fresh,
        retest_confirmed=retest_confirmed,
        acceptance_confirmed=acceptance_confirmed,
        ltf_confirmed=ltf_confirmed,
        continuation_ready_after_retest=False,
        late_entry_risk=bool(already_moved_r is not None and already_moved_r > DEFAULT_MAX_CONFIRMED_ALREADY_MOVED_R),
        already_moved_R=already_moved_r,
        entry_quality=quality,
        confidence=min(0.95, score),
        entry_model_hint=MODEL_FAILED_ACCEPTANCE_RETEST,
        stop_model_hint=STOP_BEHIND_FAILED_ACCEPTANCE_EXTREME,
        target_model_hint=TARGET_TO_VALUE_EDGE_OR_LIQUIDITY,
        retest_level=candle_signal.get("retest_level") or value_edge,
        pullback_extreme=candle_signal.get("failed_acceptance_extreme"),
        blockers=blockers,
        warnings=warnings,
        reasons=reasons,
        relevant_levels=dict(levels),
    )


def _detect_late_chase(
    payload: Mapping[str, Any],
    direction: str,
    already_moved_r: Optional[float],
) -> Optional[LtfEntryWindowResult]:
    haystack = _haystack(payload)
    late_tokens = (
        "FIRST_IMPULSE_GONE",
        "FIRST_IMPULSE_ALREADY_GONE",
        "ENTRY_WINDOW_GONE",
        "ENTRY_WINDOW_ALREADY_GONE",
        "IMPULSE_ALREADY_DELIVERED",
        "IMPULSE_DELIVERED",
        "NO_CHASE",
        "WAIT_RETEST",
        "WATCH_AFTER_RETEST_ONLY",
        "LATE_SIGNAL",
        "HARD_LATE_SIGNAL",
        "PRICE_ALREADY_MOVED",
    )
    has_late_token = any(token in haystack for token in late_tokens)
    too_moved = already_moved_r is not None and already_moved_r > DEFAULT_MAX_CONFIRMED_ALREADY_MOVED_R

    if not has_late_token and not too_moved:
        return None

    warnings: List[str] = []
    if has_late_token:
        warnings.append("Payload indicates first impulse / entry window may already be gone.")
    if too_moved:
        warnings.append(f"Price already moved {already_moved_r:.2f}R from entry reference.")

    return LtfEntryWindowResult(
        ltf_entry_window_detected=False,
        entry_window_type=ENTRY_WINDOW_LATE_CHASE,
        entry_window_state=STATE_LATE,
        entry_window_direction=direction,
        fresh_entry_window=False,
        retest_confirmed=False,
        acceptance_confirmed=False,
        ltf_confirmed=False,
        continuation_ready_after_retest=False,
        late_entry_risk=True,
        already_moved_R=already_moved_r,
        entry_quality=ENTRY_QUALITY_LATE,
        confidence=0.72 if too_moved else 0.55,
        entry_model_hint=MODEL_NO_CLEAN_ENTRY,
        blockers=["late_chase_no_fresh_retest"],
        warnings=warnings,
        reasons=["Late continuation/chase risk detected without a fresh retest entry window."],
    )


# ---------------------------------------------------------------------------
# Candle structure helpers
# ---------------------------------------------------------------------------


def _extract_ltf_candles(payload: Mapping[str, Any]) -> List[Candle]:
    """Find recent 5m/15m candles in common payload locations."""

    candidate_keys = (
        "candles_5m",
        "ltf_candles_5m",
        "bars_5m",
        "ohlcv_5m",
        "recent_5m_candles",
        "candles_15m",
        "ltf_candles_15m",
        "bars_15m",
        "ohlcv_15m",
        "recent_candles",
        "candles",
        "bars",
    )

    raw: Any = None
    for key in candidate_keys:
        value = payload.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and value:
            raw = value
            break

    if raw is None:
        timeframes = payload.get("timeframes") or payload.get("timeframe_data") or payload.get("data_by_timeframe")
        if isinstance(timeframes, Mapping):
            for tf_key in ("5m", "M5", "15m", "M15"):
                value = timeframes.get(tf_key)
                if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and value:
                    raw = value
                    break

    if raw is None:
        ltf = payload.get("ltf") or payload.get("ltf_data")
        if isinstance(ltf, Mapping):
            for key in candidate_keys:
                value = ltf.get(key)
                if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and value:
                    raw = value
                    break

    candles: List[Candle] = []
    for item in list(raw or [])[-120:]:
        c = _normalize_candle(item)
        if c is not None:
            candles.append(c)
    return candles


def _normalize_candle(item: Any) -> Optional[Candle]:
    if isinstance(item, Mapping):
        o = _safe_float(item.get("open") or item.get("o"))
        h = _safe_float(item.get("high") or item.get("h"))
        l = _safe_float(item.get("low") or item.get("l"))
        c = _safe_float(item.get("close") or item.get("c"))
        ts = item.get("timestamp") or item.get("time") or item.get("datetime") or item.get("ts")
        v = _safe_float(item.get("volume") or item.get("v"))
        if None not in (o, h, l, c):
            return Candle(open=o, high=h, low=l, close=c, timestamp=str(ts) if ts is not None else None, volume=v)
        return None

    if isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
        # Accept [ts, open, high, low, close, volume] or [open, high, low, close].
        seq = list(item)
        if len(seq) >= 6:
            ts, o, h, l, c, v = seq[:6]
            o, h, l, c, v = map(_safe_float, (o, h, l, c, v))
            if None not in (o, h, l, c):
                return Candle(open=o, high=h, low=l, close=c, timestamp=str(ts), volume=v)
        if len(seq) >= 4:
            o, h, l, c = map(_safe_float, seq[:4])
            if None not in (o, h, l, c):
                return Candle(open=o, high=h, low=l, close=c)
    return None


def _candle_sweep_reclaim_signal(
    direction: str,
    candles: Sequence[Candle],
    liquidity_level: Optional[float],
    payload: Mapping[str, Any],
) -> Dict[str, Any]:
    out = {
        "sweep": False,
        "reclaim": False,
        "displacement": False,
        "retest": False,
        "sweep_extreme": None,
        "retest_level": None,
    }
    if len(candles) < 4 or liquidity_level is None:
        return out

    recent = list(candles)[-24:]
    last = recent[-1]
    tol = _price_tolerance(payload, liquidity_level)

    if direction == "LONG":
        sweep_candidates = [c for c in recent if c.low < liquidity_level - tol]
        out["sweep"] = bool(sweep_candidates)
        if sweep_candidates:
            out["sweep_extreme"] = min(c.low for c in sweep_candidates)
        out["reclaim"] = last.close > liquidity_level + tol
        out["displacement"] = _has_bullish_displacement(recent)
        out["retest"] = any(c.low <= liquidity_level + 2 * tol and c.close >= liquidity_level for c in recent[-8:])
    else:
        sweep_candidates = [c for c in recent if c.high > liquidity_level + tol]
        out["sweep"] = bool(sweep_candidates)
        if sweep_candidates:
            out["sweep_extreme"] = max(c.high for c in sweep_candidates)
        out["reclaim"] = last.close < liquidity_level - tol
        out["displacement"] = _has_bearish_displacement(recent)
        out["retest"] = any(c.high >= liquidity_level - 2 * tol and c.close <= liquidity_level for c in recent[-8:])

    out["retest_level"] = liquidity_level
    return out


def _candle_continuation_retest_signal(
    direction: str,
    candles: Sequence[Candle],
    breakout_level: Optional[float],
    payload: Mapping[str, Any],
) -> Dict[str, Any]:
    out = {
        "pullback": False,
        "retest": False,
        "holds_structure": False,
        "continuation_trigger": False,
        "pullback_extreme": None,
        "retest_level": None,
    }
    if len(candles) < 6:
        return out

    recent = list(candles)[-30:]
    last = recent[-1]

    highs = [c.high for c in recent]
    lows = [c.low for c in recent]
    highest = max(highs)
    lowest = min(lows)

    # If no explicit breakout level exists, use a simple recent midpoint/base proxy.
    if breakout_level is None:
        breakout_level = (highest + lowest) / 2.0

    tol = _price_tolerance(payload, breakout_level)

    if direction == "LONG":
        # Pullback from recent high, then hold above level/base.
        out["pullback"] = any(c.low < highest - 3 * tol for c in recent[-12:-1])
        out["retest"] = any(c.low <= breakout_level + 2 * tol and c.close >= breakout_level - tol for c in recent[-12:])
        out["holds_structure"] = last.close >= breakout_level - tol and min(c.low for c in recent[-8:]) > lowest + tol
        out["continuation_trigger"] = _has_bullish_displacement(recent[-8:]) or last.close >= max(c.close for c in recent[-6:])
        out["pullback_extreme"] = min(c.low for c in recent[-12:])
    else:
        out["pullback"] = any(c.high > lowest + 3 * tol for c in recent[-12:-1])
        out["retest"] = any(c.high >= breakout_level - 2 * tol and c.close <= breakout_level + tol for c in recent[-12:])
        out["holds_structure"] = last.close <= breakout_level + tol and max(c.high for c in recent[-8:]) < highest - tol
        out["continuation_trigger"] = _has_bearish_displacement(recent[-8:]) or last.close <= min(c.close for c in recent[-6:])
        out["pullback_extreme"] = max(c.high for c in recent[-12:])

    out["retest_level"] = breakout_level
    return out


def _candle_failed_acceptance_signal(
    direction: str,
    candles: Sequence[Candle],
    value_edge: Optional[float],
    payload: Mapping[str, Any],
) -> Dict[str, Any]:
    out = {
        "tested_edge": False,
        "failed_acceptance": False,
        "retest": False,
        "ltf_shift": False,
        "failed_acceptance_extreme": None,
        "retest_level": None,
    }
    if len(candles) < 4 or value_edge is None:
        return out

    recent = list(candles)[-24:]
    last = recent[-1]
    tol = _price_tolerance(payload, value_edge)

    if direction == "LONG":
        tests = [c for c in recent if c.low <= value_edge + tol]
        out["tested_edge"] = bool(tests)
        out["failed_acceptance"] = bool(tests and last.close > value_edge + tol)
        out["retest"] = any(c.low <= value_edge + 2 * tol and c.close > value_edge for c in recent[-8:])
        out["ltf_shift"] = _has_bullish_displacement(recent[-8:])
        out["failed_acceptance_extreme"] = min((c.low for c in tests), default=None)
    else:
        tests = [c for c in recent if c.high >= value_edge - tol]
        out["tested_edge"] = bool(tests)
        out["failed_acceptance"] = bool(tests and last.close < value_edge - tol)
        out["retest"] = any(c.high >= value_edge - 2 * tol and c.close < value_edge for c in recent[-8:])
        out["ltf_shift"] = _has_bearish_displacement(recent[-8:])
        out["failed_acceptance_extreme"] = max((c.high for c in tests), default=None)

    out["retest_level"] = value_edge
    return out


# ---------------------------------------------------------------------------
# Level, direction, and scoring helpers
# ---------------------------------------------------------------------------


def _infer_direction(payload: Mapping[str, Any]) -> str:
    direct = str(payload.get("direction") or payload.get("side") or "").upper()
    if direct in {"LONG", "BUY", "BULL", "BULLISH"}:
        return "LONG"
    if direct in {"SHORT", "SELL", "BEAR", "BEARISH"}:
        return "SHORT"

    text = _haystack(payload)
    if any(token in text for token in ("_LONG", " LONG", "BULLISH", "BUY")):
        return "LONG"
    if any(token in text for token in ("_SHORT", " SHORT", "BEARISH", "SELL")):
        return "SHORT"
    return "UNKNOWN"


def _collect_relevant_levels(payload: Mapping[str, Any]) -> Dict[str, Any]:
    aliases = {
        "previous_high": ("previous_high", "prior_high", "prev_high", "pd_high", "session_previous_high"),
        "previous_low": ("previous_low", "prior_low", "prev_low", "pd_low", "session_previous_low"),
        "range_high": ("range_high", "balance_high", "ib_high", "initial_balance_high"),
        "range_low": ("range_low", "balance_low", "ib_low", "initial_balance_low"),
        "vah": ("vah", "VAH", "value_area_high", "prior_vah", "previous_vah"),
        "val": ("val", "VAL", "value_area_low", "prior_val", "previous_val"),
        "poc": ("poc", "POC", "prior_poc", "previous_poc", "naked_poc", "npoc"),
        "open_price": ("open", "open_price", "session_open", "market_open"),
        "breakout_level": ("breakout_level", "base_level", "retest_level", "trigger_level"),
        "liquidity_level": ("liquidity_level", "sweep_level", "reclaim_level"),
    }

    out: Dict[str, Any] = {}
    for canonical, keys in aliases.items():
        value = _first_float(payload, *keys)
        if value is not None:
            out[canonical] = value

    # Nested TPO/open context support.
    for nested_key in ("tpo", "tpo_context", "auction_context", "open_context", "metadata"):
        nested = payload.get(nested_key)
        if isinstance(nested, Mapping):
            for canonical, keys in aliases.items():
                if canonical in out:
                    continue
                value = _first_float(nested, *keys)
                if value is not None:
                    out[canonical] = value
    return out


def _choose_liquidity_level(direction: str, levels: Mapping[str, Any]) -> Optional[float]:
    if _safe_float(levels.get("liquidity_level")) is not None:
        return _safe_float(levels.get("liquidity_level"))
    if direction == "LONG":
        for key in ("previous_low", "range_low", "val"):
            value = _safe_float(levels.get(key))
            if value is not None:
                return value
    if direction == "SHORT":
        for key in ("previous_high", "range_high", "vah"):
            value = _safe_float(levels.get(key))
            if value is not None:
                return value
    return None


def _choose_value_edge(direction: str, levels: Mapping[str, Any]) -> Optional[float]:
    if direction == "LONG":
        for key in ("val", "range_low", "previous_low", "breakout_level"):
            value = _safe_float(levels.get(key))
            if value is not None:
                return value
    if direction == "SHORT":
        for key in ("vah", "range_high", "previous_high", "breakout_level"):
            value = _safe_float(levels.get(key))
            if value is not None:
                return value
    return None


def _choose_breakout_or_base_level(direction: str, levels: Mapping[str, Any], payload: Mapping[str, Any]) -> Optional[float]:
    explicit = _safe_float(levels.get("breakout_level"))
    if explicit is not None:
        return explicit

    entry = _first_float(payload, "entry_reference_price", "entry", "entry_price", "planned_entry")
    if entry is not None:
        return entry

    if direction == "LONG":
        for key in ("previous_high", "range_high", "vah", "poc"):
            value = _safe_float(levels.get(key))
            if value is not None:
                return value
    if direction == "SHORT":
        for key in ("previous_low", "range_low", "val", "poc"):
            value = _safe_float(levels.get(key))
            if value is not None:
                return value
    return None


def _choose_best_candidate(candidates: Sequence[LtfEntryWindowResult]) -> LtfEntryWindowResult:
    def rank(item: LtfEntryWindowResult) -> Tuple[int, float]:
        state_rank = {
            STATE_CONFIRMED: 4,
            STATE_FORMING: 3,
            STATE_PENDING_RETEST: 2,
            STATE_LATE: 1,
            STATE_NONE: 0,
            STATE_INVALID: 0,
        }.get(item.entry_window_state, 0)
        fresh_bonus = 1 if item.fresh_entry_window else 0
        return state_rank + fresh_bonus, item.confidence

    return sorted(candidates, key=rank, reverse=True)[0]


def _compute_already_moved_r(
    direction: str,
    entry: Optional[float],
    stop: Optional[float],
    current: Optional[float],
    payload: Mapping[str, Any],
) -> Optional[float]:
    explicit = _first_float(payload, "already_moved_R", "already_moved_r", "impulse_progress_R")
    if explicit is not None:
        return explicit
    if direction not in {"LONG", "SHORT"} or entry is None or stop is None or current is None:
        return None
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    if direction == "LONG":
        return max(0.0, (current - entry) / risk)
    return max(0.0, (entry - current) / risk)


def _has_bullish_displacement(candles: Sequence[Candle]) -> bool:
    if len(candles) < 3:
        return False
    recent = list(candles)[-5:]
    ranges = [max(1e-12, c.high - c.low) for c in recent]
    avg_range = sum(ranges[:-1]) / max(1, len(ranges[:-1]))
    last = recent[-1]
    body = last.close - last.open
    return body > 0 and body > avg_range * 0.35 and last.close >= max(c.close for c in recent[:-1])


def _has_bearish_displacement(candles: Sequence[Candle]) -> bool:
    if len(candles) < 3:
        return False
    recent = list(candles)[-5:]
    ranges = [max(1e-12, c.high - c.low) for c in recent]
    avg_range = sum(ranges[:-1]) / max(1, len(ranges[:-1]))
    last = recent[-1]
    body = last.open - last.close
    return body > 0 and body > avg_range * 0.35 and last.close <= min(c.close for c in recent[:-1])


def _price_tolerance(payload: Mapping[str, Any], price: Optional[float]) -> float:
    explicit = _first_float(payload, "entry_window_tolerance", "price_tolerance", "tick_tolerance", "atr_tolerance")
    if explicit is not None and explicit > 0:
        return explicit
    atr = _first_float(payload, "atr_5m", "atr_15m", "atr", "average_true_range")
    if atr is not None and atr > 0:
        return atr * 0.15
    if price is None or price <= 0:
        return DEFAULT_SYMBOL_TOLERANCE_PCT
    return abs(price) * DEFAULT_SYMBOL_TOLERANCE_PCT


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except Exception:
        return None


def _first_float(mapping: Mapping[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        if key in mapping:
            value = _safe_float(mapping.get(key))
            if value is not None:
                return value
    return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on", "confirmed", "ok", "valid"}


def _any_bool(mapping: Mapping[str, Any], *keys: str) -> bool:
    for key in keys:
        if _to_bool(mapping.get(key)):
            return True
    metadata = mapping.get("metadata")
    if isinstance(metadata, Mapping):
        for key in keys:
            if _to_bool(metadata.get(key)):
                return True
    ltf = mapping.get("ltf") or mapping.get("ltf_model") or mapping.get("ltf_entry")
    if isinstance(ltf, Mapping):
        for key in keys:
            if _to_bool(ltf.get(key)):
                return True
    return False


def _haystack(payload: Mapping[str, Any]) -> str:
    parts: List[str] = []

    def add(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, Mapping):
            for k, v in value.items():
                parts.append(str(k))
                add(v)
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                add(item)
            return
        parts.append(str(value))

    for key in (
        "symbol",
        "scenario",
        "scenario_type",
        "direction",
        "htf_bias",
        "alignment",
        "open_behavior",
        "open_context",
        "open_relation",
        "auction_bias",
        "market_state",
        "tpo_watch_setup",
        "tpo_watch_state",
        "tpo_signal_permission",
        "ltf_model_state",
        "ltf_model_result",
        "entry_model_hint",
        "trigger_reason",
        "risk_mode",
        "entry_timing_status",
        "flags",
        "caution_flags",
        "blockers",
        "warnings",
        "metadata",
    ):
        add(payload.get(key))

    return " ".join(parts).upper()


def _token_score(haystack: str, tokens: Iterable[str]) -> int:
    return sum(1 for token in tokens if token.upper() in haystack)


def _base_debug(
    payload: Mapping[str, Any],
    candles: Sequence[Candle],
    direction: str,
    levels: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "detected_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": payload.get("symbol"),
        "direction": direction,
        "candles_seen": len(candles),
        "levels_seen": sorted(levels.keys()),
        "detector_version": LTF_ENTRY_WINDOW_DETECTOR_VERSION,
    }


__all__ = [
    "LTF_ENTRY_WINDOW_DETECTOR_VERSION",
    "ENTRY_WINDOW_NONE",
    "ENTRY_WINDOW_SWEEP_RECLAIM_RETEST",
    "ENTRY_WINDOW_CONTINUATION_RETEST",
    "ENTRY_WINDOW_FAILED_ACCEPTANCE_RETEST",
    "ENTRY_WINDOW_LATE_CHASE",
    "ENTRY_WINDOW_PULLBACK_PENDING",
    "ENTRY_WINDOW_RETEST_PENDING",
    "ENTRY_WINDOW_CONFIRMED",
    "ENTRY_WINDOW_EXHAUSTED",
    "STATE_NONE",
    "STATE_FORMING",
    "STATE_PENDING_RETEST",
    "STATE_CONFIRMED",
    "STATE_LATE",
    "STATE_INVALID",
    "ENTRY_QUALITY_GOOD",
    "ENTRY_QUALITY_WEAK",
    "ENTRY_QUALITY_LATE",
    "ENTRY_QUALITY_INVALID",
    "ENTRY_QUALITY_UNKNOWN",
    "detect_ltf_entry_window",
    "evaluate_ltf_entry_window",
    "detect",
]
