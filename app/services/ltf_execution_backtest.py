from __future__ import annotations

"""Deterministic no-look-ahead backtest for LTF Execution State Machine v2.

The historical candidate layer reconstructs only the two auction families that
can be defined from OHLC history without inventing macro or positioning data:

- true Open Test Drive (outside prior value -> test -> failed acceptance);
- Open Rejection Reverse (outside prior value -> acceptance back inside).

The live state machine supports additional TPO-watch families, but they are not
silently approximated in this historical test.  This is an execution-layer
backtest, not a claim that historical macro/Battle Gate state was known.
"""

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
import math
from statistics import median
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from app.services.ltf_execution_state_machine import (
    LTF_EXECUTION_STATE_MACHINE_VERSION,
    LTFExecutionStateStore,
    STATE_ENTRY_READY,
)


LTF_EXECUTION_BACKTEST_VERSION = (
    "ltf-execution-v2-backtest-v1.0-reconstructed-tpo-walk-forward"
)

MIN_PRIOR_SESSION_M5_BARS = 96
MIN_CURRENT_SESSION_M5_BARS = 24
FIRST_ACTIVITY_MINUTES = 90
DEFAULT_EXECUTION_HORIZON_HOURS = 8
DEFAULT_PREHISTORY_HOURS = 4


@dataclass(frozen=True, slots=True)
class SessionSpec:
    timezone: str
    open_time: time
    label: str
    max_horizon_hours: int = DEFAULT_EXECUTION_HORIZON_HOURS


DEFAULT_SESSION_SPEC = SessionSpec(
    timezone="UTC",
    open_time=time(0, 0),
    label="UTC_DAY",
)

SESSION_SPECS: dict[str, SessionSpec] = {
    "BTCUSD": SessionSpec("UTC", time(0, 0), "UTC_CRYPTO_DAY", 12),
    "ETHUSD": SessionSpec("UTC", time(0, 0), "UTC_CRYPTO_DAY", 12),
    "XAUUSD": SessionSpec("Europe/London", time(8, 0), "LONDON_SYNTHETIC", 8),
    "EURUSD": SessionSpec("Europe/London", time(8, 0), "LONDON_SYNTHETIC", 8),
    "GBPUSD": SessionSpec("Europe/London", time(8, 0), "LONDON_SYNTHETIC", 8),
    "USDCHF": SessionSpec("Europe/London", time(8, 0), "LONDON_SYNTHETIC", 8),
    "USDJPY": SessionSpec("Asia/Tokyo", time(9, 0), "TOKYO_SYNTHETIC", 8),
    "AUDUSD": SessionSpec("Asia/Tokyo", time(9, 0), "ASIA_SYNTHETIC", 8),
    "USDCAD": SessionSpec("America/New_York", time(8, 0), "NY_SYNTHETIC", 8),
    "GER40": SessionSpec("Europe/Berlin", time(9, 0), "XETRA_CASH", 8),
    "NAS100": SessionSpec("America/New_York", time(9, 30), "NY_RTH", 7),
    "SPX500": SessionSpec("America/New_York", time(9, 30), "NY_RTH", 7),
    "UKOIL": SessionSpec("Europe/London", time(8, 0), "LONDON_SYNTHETIC", 9),
}

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


@dataclass(frozen=True, slots=True)
class ReconstructedProfile:
    session_id: str
    session_open_utc: datetime
    session_close_utc: datetime
    high: float
    low: float
    vah: float
    val: float
    poc: float
    bin_width: float
    bars: int

    def to_interest_zones(self) -> list[dict[str, Any]]:
        return [
            {
                "zone_type": "SESSION_HIGH",
                "role": "STRUCTURAL_INTEREST",
                "price": self.high,
                "profile_id": self.session_id,
            },
            {
                "zone_type": "SESSION_LOW",
                "role": "STRUCTURAL_INTEREST",
                "price": self.low,
                "profile_id": self.session_id,
            },
            {
                "zone_type": "VAH",
                "role": "VALUE_EDGE",
                "price": self.vah,
                "profile_id": self.session_id,
            },
            {
                "zone_type": "VAL",
                "role": "VALUE_EDGE",
                "price": self.val,
                "profile_id": self.session_id,
            },
            {
                "zone_type": "POC",
                "role": "FAIR_VALUE",
                "price": self.poc,
                "profile_id": self.session_id,
            },
        ]


@dataclass(frozen=True, slots=True)
class HistoricalWatchCandidate:
    candidate_id: str
    symbol: str
    session_id: str
    reference_profile_id: str
    setup_family: str
    direction: str
    session_open_utc: datetime
    activated_at_utc: datetime
    expires_at_utc: datetime
    open_price: float
    previous_vah: float
    previous_val: float
    previous_poc: float
    previous_high: float
    previous_low: float
    test_extreme: float
    htf_bias: str
    interest_zones: tuple[dict[str, Any], ...]
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        for key in (
            "session_open_utc",
            "activated_at_utc",
            "expires_at_utc",
        ):
            data[key] = data[key].isoformat()
        data["interest_zones"] = list(self.interest_zones)
        return data


def _as_utc(value: Any) -> datetime:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.to_pydatetime()


def normalize_m5_history(frame: pd.DataFrame, *, symbol: str) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    out.columns = [str(column).lower() for column in out.columns]
    required = {"open", "high", "low", "close"}
    missing = sorted(required.difference(out.columns))
    if missing:
        raise ValueError(f"{symbol} M5 history is missing OHLC columns: {missing}")

    if "bar_open_utc" in out.columns:
        opens = pd.to_datetime(out["bar_open_utc"], utc=True, errors="coerce")
    elif "datetime" in out.columns:
        opens = pd.to_datetime(out["datetime"], utc=True, errors="coerce")
    elif "timestamp" in out.columns:
        opens = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    else:
        opens = pd.to_datetime(out.index, utc=True, errors="coerce")
    out["bar_open_utc"] = opens
    out["bar_close_utc"] = opens + pd.Timedelta(minutes=5)
    for column in required:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    if "volume" not in out.columns:
        out["volume"] = 0.0
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0.0)
    out["symbol"] = symbol.upper()
    out = out.dropna(
        subset=["bar_open_utc", "open", "high", "low", "close"]
    )
    out = out.sort_values("bar_open_utc")
    out = out.drop_duplicates(subset=["bar_open_utc"], keep="last")
    return out.reset_index(drop=True)


def _session_open(local_day: date, spec: SessionSpec) -> datetime:
    zone = ZoneInfo(spec.timezone)
    local = datetime.combine(local_day, spec.open_time, tzinfo=zone)
    return local.astimezone(UTC)


def _session_days(frame: pd.DataFrame, spec: SessionSpec) -> list[date]:
    if frame.empty:
        return []
    zone = ZoneInfo(spec.timezone)
    first = _as_utc(frame["bar_open_utc"].iloc[0]).astimezone(zone).date()
    last = _as_utc(frame["bar_open_utc"].iloc[-1]).astimezone(zone).date()
    return list(pd.date_range(first, last, freq="D").date)


def _slice(
    frame: pd.DataFrame,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    opens = pd.to_datetime(frame["bar_open_utc"], utc=True)
    mask = (opens >= pd.Timestamp(start)) & (opens < pd.Timestamp(end))
    return frame.loc[mask].copy()


def _resample_m15(frame: pd.DataFrame, *, origin: datetime) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    indexed = frame.set_index("bar_open_utc")
    result = indexed.resample(
        "15min",
        origin=pd.Timestamp(origin),
        label="left",
        closed="left",
    ).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    return result.dropna(subset=["open", "high", "low", "close"])


def _profile_from_session(
    frame: pd.DataFrame,
    *,
    symbol: str,
    session_id: str,
    session_open: datetime,
    session_close: datetime,
) -> ReconstructedProfile | None:
    m15 = _resample_m15(frame, origin=session_open)
    if len(m15) < 16:
        return None

    ranges = (m15["high"] - m15["low"]).abs()
    median_range = float(ranges.median())
    tick = TICK_SIZE_BY_SYMBOL.get(symbol, max(abs(float(m15["close"].iloc[-1])) * 0.00001, 0.00001))
    bin_width = max(tick * 4.0, median_range * 0.25)
    # A TPO profile counts every price bracket traversed by each M15 period.
    # Using only one typical-price point per bar would systematically compress
    # value area and create false outside-value opens in the replay.
    bracket_counts: Counter[int] = Counter()
    for _, row in m15.iterrows():
        low_index = math.floor(float(row["low"]) / bin_width)
        high_index = math.ceil(float(row["high"]) / bin_width)
        for bracket_index in range(low_index, high_index + 1):
            bracket_counts[bracket_index] += 1
    if not bracket_counts:
        return None

    ordered_indices = sorted(bracket_counts)
    poc_index_value = max(
        ordered_indices,
        key=lambda bracket_index: (
            bracket_counts[bracket_index],
            -abs(
                (bracket_index * bin_width)
                - float(m15["close"].iloc[-1])
            ),
        ),
    )
    poc = float(poc_index_value * bin_width)
    total = float(sum(bracket_counts.values()))
    selected = {poc}
    ordered = [float(value * bin_width) for value in ordered_indices]
    poc_index = ordered_indices.index(poc_index_value)
    low_index = poc_index
    high_index = poc_index
    accumulated = float(bracket_counts[poc_index_value])
    while accumulated / total < 0.70 and (
        low_index > 0 or high_index < len(ordered) - 1
    ):
        lower_count = (
            float(bracket_counts[ordered_indices[low_index - 1]])
            if low_index > 0
            else -1.0
        )
        upper_count = (
            float(bracket_counts[ordered_indices[high_index + 1]])
            if high_index < len(ordered) - 1
            else -1.0
        )
        if upper_count >= lower_count and high_index < len(ordered) - 1:
            high_index += 1
            selected.add(ordered[high_index])
            accumulated += max(0.0, upper_count)
        elif low_index > 0:
            low_index -= 1
            selected.add(ordered[low_index])
            accumulated += max(0.0, lower_count)
        else:
            break

    return ReconstructedProfile(
        session_id=session_id,
        session_open_utc=session_open,
        session_close_utc=session_close,
        high=float(m15["high"].max()),
        low=float(m15["low"].min()),
        vah=max(selected),
        val=min(selected),
        poc=poc,
        bin_width=bin_width,
        bars=len(m15),
    )


def _htf_bias(frame: pd.DataFrame, *, as_of: datetime) -> str:
    history = frame.loc[
        pd.to_datetime(frame["bar_close_utc"], utc=True) <= pd.Timestamp(as_of)
    ]
    if len(history) < 240:
        return "NEUTRAL"
    indexed = history.set_index("bar_open_utc")
    h4 = indexed.resample("4h", label="right", closed="right").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
        }
    )
    h4 = h4.dropna(subset=["close"])
    if len(h4) < 24:
        return "NEUTRAL"
    ema = h4["close"].ewm(span=20, adjust=False).mean()
    last_close = float(h4["close"].iloc[-1])
    last_ema = float(ema.iloc[-1])
    slope = float(ema.iloc[-1] - ema.iloc[-4])
    tolerance = max(abs(last_ema) * 0.0002, float((h4["high"] - h4["low"]).tail(20).median()) * 0.05)
    if last_close > last_ema + tolerance and slope > 0:
        return "LONG"
    if last_close < last_ema - tolerance and slope < 0:
        return "SHORT"
    return "NEUTRAL"


def _two_block_acceptance(
    post_touch: pd.DataFrame,
    *,
    edge: float,
    opened_above: bool,
    origin: datetime,
) -> tuple[bool, datetime | None]:
    if post_touch.empty:
        return False, None
    indexed = post_touch.set_index("bar_open_utc")
    blocks = indexed.resample(
        "30min",
        origin=pd.Timestamp(origin),
        label="left",
        closed="left",
    ).agg({"close": "last"})
    blocks = blocks.dropna(subset=["close"])
    if len(blocks) < 2:
        return False, None
    inside = (
        blocks["close"] < edge
        if opened_above
        else blocks["close"] > edge
    )
    for index in range(1, len(blocks)):
        if bool(inside.iloc[index - 1]) and bool(inside.iloc[index]):
            activated = _as_utc(blocks.index[index] + pd.Timedelta(minutes=30))
            return True, activated
    return False, None


def _candidate_interest_zones(
    profiles: Sequence[ReconstructedProfile],
) -> tuple[dict[str, Any], ...]:
    zones: list[dict[str, Any]] = []
    seen: set[tuple[str, float]] = set()
    for profile in list(profiles)[-5:]:
        for zone in profile.to_interest_zones():
            key = (str(zone["zone_type"]), round(float(zone["price"]), 8))
            if key in seen:
                continue
            seen.add(key)
            zones.append(zone)
    return tuple(zones)


def reconstruct_tpo_watch_candidates(
    frame: pd.DataFrame,
    *,
    symbol: str,
) -> tuple[list[HistoricalWatchCandidate], dict[str, Any]]:
    symbol = symbol.upper()
    history = normalize_m5_history(frame, symbol=symbol)
    spec = SESSION_SPECS.get(symbol, DEFAULT_SESSION_SPEC)
    days = _session_days(history, spec)
    candidates: list[HistoricalWatchCandidate] = []
    diagnostics: Counter[str] = Counter()

    # Build profiles once, then resolve the latest completed *trading* session
    # for each day.  This makes Monday use Friday for weekday instruments and
    # also handles exchange holidays without peeking into the current session.
    completed_profiles: list[ReconstructedProfile] = []
    for local_day in days:
        session_open = _session_open(local_day, spec)
        session_close = _session_open(local_day + timedelta(days=1), spec)
        session_frame = _slice(history, session_open, session_close)
        if len(session_frame) < MIN_PRIOR_SESSION_M5_BARS:
            continue
        profile = _profile_from_session(
            session_frame,
            symbol=symbol,
            session_id=f"{symbol}_{local_day.isoformat()}_{spec.label}",
            session_open=session_open,
            session_close=session_close,
        )
        if profile is not None:
            completed_profiles.append(profile)

    for local_day in days:
        current_open = _session_open(local_day, spec)
        next_open = _session_open(local_day + timedelta(days=1), spec)
        current_frame = _slice(history, current_open, next_open)
        prior_profiles = [
            profile
            for profile in completed_profiles
            if profile.session_close_utc <= current_open
        ]
        if not prior_profiles:
            diagnostics["skipped_prior_coverage"] += 1
            continue
        if len(current_frame) < MIN_CURRENT_SESSION_M5_BARS:
            diagnostics["skipped_current_coverage"] += 1
            continue

        previous_profile = prior_profiles[-1]
        previous_frame = _slice(
            history,
            previous_profile.session_open_utc,
            previous_profile.session_close_utc,
        )

        first_activity_end = current_open + timedelta(minutes=FIRST_ACTIVITY_MINUTES)
        first_activity = _slice(current_frame, current_open, first_activity_end)
        if len(first_activity) < 12:
            diagnostics["skipped_first_activity_coverage"] += 1
            continue

        open_price = float(first_activity.iloc[0]["open"])
        ranges = (previous_frame["high"] - previous_frame["low"]).abs()
        median_range = float(ranges.tail(120).median())
        tick = TICK_SIZE_BY_SYMBOL.get(symbol, max(abs(open_price) * 0.00001, 0.00001))
        tolerance = max(tick * 3.0, median_range * 0.10)
        opened_above = open_price > previous_profile.vah + tolerance
        opened_below = open_price < previous_profile.val - tolerance
        if not opened_above and not opened_below:
            diagnostics["skipped_open_inside_value"] += 1
            continue

        edge = previous_profile.vah if opened_above else previous_profile.val
        touches = (
            first_activity["low"] <= edge + tolerance
            if opened_above
            else first_activity["high"] >= edge - tolerance
        )
        if not bool(touches.any()):
            diagnostics["skipped_no_value_test"] += 1
            continue
        touch_position = int(touches.to_numpy().argmax())
        post_touch = first_activity.iloc[touch_position:].copy()
        touch_bar = post_touch.iloc[0]
        touch_close = _as_utc(touch_bar["bar_close_utc"])

        accepted_inside, accepted_at = _two_block_acceptance(
            post_touch,
            edge=edge,
            opened_above=opened_above,
            origin=current_open,
        )

        rejection_mask = (
            (post_touch["close"] >= edge + tolerance)
            & (post_touch["close"] > post_touch["open"])
            if opened_above
            else (post_touch["close"] <= edge - tolerance)
            & (post_touch["close"] < post_touch["open"])
        )
        rejection_rows = post_touch.loc[rejection_mask]
        rejection_at = (
            _as_utc(rejection_rows.iloc[0]["bar_close_utc"])
            if not rejection_rows.empty
            else None
        )

        if (
            rejection_at is not None
            and (accepted_at is None or rejection_at < accepted_at)
        ):
            setup_family = "OPEN_TEST_DRIVE"
            direction = "LONG" if opened_above else "SHORT"
            activated_at = max(touch_close, rejection_at)
            value_state = "REJECTED_BACK_OUTSIDE_VALUE"
            current_behavior = "OPEN_TEST_DRIVE_CONFIRMED"
            test_window = post_touch.loc[
                pd.to_datetime(post_touch["bar_close_utc"], utc=True)
                <= pd.Timestamp(activated_at)
            ]
            test_extreme = (
                float(test_window["low"].min())
                if direction == "LONG"
                else float(test_window["high"].max())
            )
            diagnostics["otd_candidates"] += 1
        elif accepted_inside and accepted_at is not None:
            setup_family = "OPEN_REJECTION_REVERSE"
            direction = "SHORT" if opened_above else "LONG"
            activated_at = accepted_at
            value_state = "REJECTED_BACK_INTO_PRIOR_VALUE"
            current_behavior = "OPEN_REJECTION_REVERSE"
            test_window = post_touch.loc[
                pd.to_datetime(post_touch["bar_close_utc"], utc=True)
                <= pd.Timestamp(activated_at)
            ]
            test_extreme = (
                float(test_window["high"].max())
                if direction == "SHORT"
                else float(test_window["low"].min())
            )
            diagnostics["orr_candidates"] += 1
        else:
            diagnostics["skipped_no_confirmed_branch"] += 1
            continue

        bias = _htf_bias(history, as_of=activated_at)
        if bias in {"LONG", "SHORT"} and bias != direction:
            diagnostics["skipped_counter_htf"] += 1
            continue

        expires_at = min(
            next_open,
            activated_at + timedelta(hours=spec.max_horizon_hours),
        )
        session_id = f"{symbol}_{local_day.isoformat()}_{spec.label}"
        candidate_id = (
            f"{session_id}_{setup_family}_{direction}_{activated_at:%H%M}"
        )
        zones = _candidate_interest_zones(prior_profiles)
        payload = {
            "symbol": symbol,
            "direction": direction,
            "expected_direction": direction,
            "htf_bias": bias,
            "signal_alignment": (
                "TREND_ALIGNED" if bias == direction else "NEUTRAL_HTF_OTD"
            ),
            "tpo_watch_state": "LTF_MODEL_PENDING",
            "tpo_watch_active": True,
            "tpo_watch_setup": current_behavior,
            "open_behavior": setup_family,
            "current_open_behavior": current_behavior,
            "open_location": (
                "OPEN_ABOVE_VALUE" if opened_above else "OPEN_BELOW_VALUE"
            ),
            "value_acceptance_state": value_state,
            "value_test_occurred": True,
            "value_test_level": "VAH" if opened_above else "VAL",
            "value_rejection_confirmed": setup_family == "OPEN_TEST_DRIVE",
            "current_session_id": session_id,
            "reference_profile_id": previous_profile.session_id,
            "session_scope": spec.label,
            "previous_vah": previous_profile.vah,
            "previous_val": previous_profile.val,
            "previous_poc": previous_profile.poc,
            "previous_high": previous_profile.high,
            "previous_low": previous_profile.low,
            "current_open": open_price,
            "context_invalidation_price": test_extreme,
            "interest_zones": list(zones),
            "cycle_id": activated_at.isoformat(),
            "signal_created_at_utc": activated_at.isoformat(),
            "expires_at_utc": expires_at.isoformat(),
            "historical_context_mode": "RECONSTRUCTED_TPO_NO_LOOKAHEAD",
            "macro_guard_status": "NOT_RECONSTRUCTED",
            "positioning_status": "NOT_RECONSTRUCTED",
        }
        candidates.append(
            HistoricalWatchCandidate(
                candidate_id=candidate_id,
                symbol=symbol,
                session_id=session_id,
                reference_profile_id=previous_profile.session_id,
                setup_family=setup_family,
                direction=direction,
                session_open_utc=current_open,
                activated_at_utc=activated_at,
                expires_at_utc=expires_at,
                open_price=open_price,
                previous_vah=previous_profile.vah,
                previous_val=previous_profile.val,
                previous_poc=previous_profile.poc,
                previous_high=previous_profile.high,
                previous_low=previous_profile.low,
                test_extreme=test_extreme,
                htf_bias=bias,
                interest_zones=zones,
                payload=payload,
            )
        )

    candidates.sort(key=lambda item: (item.activated_at_utc, item.candidate_id))
    return candidates, {
        "symbol": symbol,
        "session_spec": {
            "timezone": spec.timezone,
            "open_time": spec.open_time.isoformat(),
            "label": spec.label,
        },
        "history_rows": len(history),
        "history_first_bar_utc": (
            _as_utc(history["bar_open_utc"].iloc[0]).isoformat()
            if not history.empty
            else None
        ),
        "history_last_bar_utc": (
            _as_utc(history["bar_open_utc"].iloc[-1]).isoformat()
            if not history.empty
            else None
        ),
        "candidate_count": len(candidates),
        "diagnostics": dict(sorted(diagnostics.items())),
    }


def _simulate_limit_outcome(
    bars: pd.DataFrame,
    *,
    direction: str,
    ready_at: datetime,
    expires_at: datetime,
    entry: float,
    stop: float,
    target: float,
) -> dict[str, Any]:
    risk = abs(entry - stop)
    if risk <= 0:
        return {"outcome": "INVALID_GEOMETRY", "realized_R": None}
    forward = bars.loc[
        (pd.to_datetime(bars["bar_close_utc"], utc=True) > pd.Timestamp(ready_at))
        & (
            pd.to_datetime(bars["bar_close_utc"], utc=True)
            <= pd.Timestamp(expires_at)
        )
    ]
    filled_at: datetime | None = None
    fill_bar_utc: str | None = None
    last_close = entry

    for _, row in forward.iterrows():
        bar_close = _as_utc(row["bar_close_utc"])
        high = float(row["high"])
        low = float(row["low"])
        last_close = float(row["close"])
        touches_entry = low <= entry <= high
        touches_stop = low <= stop if direction == "LONG" else high >= stop
        touches_target = high >= target if direction == "LONG" else low <= target

        if filled_at is None:
            if touches_entry and touches_target:
                return {
                    "outcome": "AMBIGUOUS_ENTRY_AND_TARGET_SAME_BAR",
                    "realized_R": None,
                    "filled_at_utc": None,
                    "resolved_at_utc": bar_close.isoformat(),
                }
            if not touches_entry:
                if touches_target:
                    return {
                        "outcome": "MISSED_TARGET_BEFORE_ENTRY",
                        "realized_R": 0.0,
                        "filled_at_utc": None,
                        "resolved_at_utc": bar_close.isoformat(),
                    }
                continue
            filled_at = bar_close
            fill_bar_utc = bar_close.isoformat()

        if touches_stop and touches_target:
            return {
                "outcome": "SL_HIT_AMBIGUOUS_BAR_CONSERVATIVE",
                "realized_R": -1.0,
                "filled_at_utc": fill_bar_utc,
                "resolved_at_utc": bar_close.isoformat(),
            }
        if touches_stop:
            return {
                "outcome": "SL_HIT",
                "realized_R": -1.0,
                "filled_at_utc": fill_bar_utc,
                "resolved_at_utc": bar_close.isoformat(),
            }
        if touches_target:
            reward_r = abs(target - entry) / risk
            return {
                "outcome": "TP_HIT",
                "realized_R": reward_r,
                "filled_at_utc": fill_bar_utc,
                "resolved_at_utc": bar_close.isoformat(),
            }

    if filled_at is None:
        return {
            "outcome": "EXPIRED_UNFILLED",
            "realized_R": 0.0,
            "filled_at_utc": None,
            "resolved_at_utc": expires_at.isoformat(),
        }
    mark_r = (
        (last_close - entry) / risk
        if direction == "LONG"
        else (entry - last_close) / risk
    )
    return {
        "outcome": "EXPIRED_AFTER_ENTRY",
        "realized_R": max(-1.0, min(mark_r, abs(target - entry) / risk)),
        "filled_at_utc": fill_bar_utc,
        "resolved_at_utc": expires_at.isoformat(),
    }


def replay_candidate(
    candidate: HistoricalWatchCandidate,
    frame: pd.DataFrame,
) -> dict[str, Any]:
    history = normalize_m5_history(frame, symbol=candidate.symbol)
    replay_start = candidate.activated_at_utc - timedelta(
        hours=DEFAULT_PREHISTORY_HOURS
    )
    replay = _slice(
        history,
        replay_start,
        candidate.expires_at_utc,
    )
    store = LTFExecutionStateStore(path=None)
    result = store.evaluate(
        candidate.payload,
        df_5m=replay,
        as_of=candidate.activated_at_utc,
        persist=False,
    )
    ready_result: dict[str, Any] | None = None
    evaluation_count = 1

    future_closes = sorted(
        {
            _as_utc(value)
            for value in replay["bar_close_utc"]
            if candidate.activated_at_utc < _as_utc(value) <= candidate.expires_at_utc
        }
    )
    for bar_close in future_closes:
        result = store.evaluate(
            candidate.payload,
            df_5m=replay,
            as_of=bar_close,
            persist=False,
        )
        evaluation_count += 1
        if result.get("ltf_execution_v2_state") == STATE_ENTRY_READY:
            ready_result = result
            break

    row: dict[str, Any] = {
        "backtest_version": LTF_EXECUTION_BACKTEST_VERSION,
        "engine_version": LTF_EXECUTION_STATE_MACHINE_VERSION,
        "candidate_id": candidate.candidate_id,
        "symbol": candidate.symbol,
        "setup_family": candidate.setup_family,
        "direction": candidate.direction,
        "htf_bias": candidate.htf_bias,
        "activated_at_utc": candidate.activated_at_utc.isoformat(),
        "expires_at_utc": candidate.expires_at_utc.isoformat(),
        "evaluation_count": evaluation_count,
        "final_ltf_state": result.get("ltf_execution_v2_state"),
        "final_ltf_outcome": result.get("ltf_model_outcome"),
        "ready": ready_result is not None,
        "ready_at_utc": None,
        "minutes_to_ready": None,
        "entry_reference_price": None,
        "invalidation_reference_price": None,
        "target_reference_price": None,
        "risk_reward_ratio": None,
        "outcome": "NO_ENTRY_READY",
        "realized_R": 0.0,
        "filled_at_utc": None,
        "resolved_at_utc": candidate.expires_at_utc.isoformat(),
        "blockers": result.get("ltf_model_blockers") or [],
        "transition_history": result.get(
            "ltf_execution_v2_transition_history"
        )
        or [],
    }
    if ready_result is None:
        return row

    ready_at = _as_utc(ready_result["ltf_execution_v2_entry_ready_at_utc"])
    entry = float(ready_result["entry_reference_price"])
    stop = float(ready_result["invalidation_reference_price"])
    target = float(ready_result["target_reference_price"])
    outcome = _simulate_limit_outcome(
        replay,
        direction=candidate.direction,
        ready_at=ready_at,
        expires_at=candidate.expires_at_utc,
        entry=entry,
        stop=stop,
        target=target,
    )
    row.update(
        {
            "ready_at_utc": ready_at.isoformat(),
            "minutes_to_ready": round(
                (ready_at - candidate.activated_at_utc).total_seconds() / 60.0,
                4,
            ),
            "entry_reference_price": entry,
            "invalidation_reference_price": stop,
            "target_reference_price": target,
            "risk_reward_ratio": float(ready_result["risk_reward_ratio"]),
            "outcome": outcome["outcome"],
            "realized_R": outcome["realized_R"],
            "filled_at_utc": outcome.get("filled_at_utc"),
            "resolved_at_utc": outcome.get("resolved_at_utc"),
            "blockers": ready_result.get("ltf_model_blockers") or [],
            "transition_history": ready_result.get(
                "ltf_execution_v2_transition_history"
            )
            or [],
        }
    )
    return row


def _wilson_interval(wins: int, losses: int, z: float = 1.96) -> list[float] | None:
    total = wins + losses
    if total <= 0:
        return None
    p = wins / total
    denominator = 1.0 + (z * z / total)
    center = (p + z * z / (2.0 * total)) / denominator
    margin = (
        z
        * math.sqrt((p * (1.0 - p) / total) + (z * z / (4.0 * total * total)))
        / denominator
    )
    return [max(0.0, center - margin), min(1.0, center + margin)]


def summarize_backtest(
    rows: Sequence[Mapping[str, Any]],
    *,
    window_start: datetime | None = None,
    window_end: datetime | None = None,
) -> dict[str, Any]:
    records = list(rows)
    ready = [row for row in records if bool(row.get("ready"))]
    filled = [row for row in ready if row.get("filled_at_utc")]
    wins = sum(1 for row in filled if row.get("outcome") == "TP_HIT")
    losses = sum(
        1
        for row in filled
        if str(row.get("outcome") or "").startswith("SL_HIT")
    )
    closed = wins + losses
    r_values = [
        float(row["realized_R"])
        for row in filled
        if row.get("realized_R") is not None
    ]
    positive_r = sum(value for value in r_values if value > 0)
    negative_r = abs(sum(value for value in r_values if value < 0))
    minutes = [
        float(row["minutes_to_ready"])
        for row in ready
        if row.get("minutes_to_ready") is not None
    ]
    outcome_counts = Counter(str(row.get("outcome") or "UNKNOWN") for row in records)
    state_counts = Counter(
        str(row.get("final_ltf_state") or "UNKNOWN") for row in records
    )
    blocker_counts: Counter[str] = Counter()
    for row in records:
        for blocker in row.get("blockers") or []:
            blocker_counts[str(blocker)] += 1

    if window_start is None and records:
        window_start = min(_as_utc(row["activated_at_utc"]) for row in records)
    if window_end is None and records:
        window_end = max(_as_utc(row["expires_at_utc"]) for row in records)
    weeks = (
        max((window_end - window_start).total_seconds() / 604800.0, 1 / 7)
        if window_start is not None and window_end is not None
        else None
    )

    return {
        "candidate_count": len(records),
        "ready_count": len(ready),
        "ready_rate": (len(ready) / len(records)) if records else None,
        "filled_count": len(filled),
        "fill_rate_of_ready": (len(filled) / len(ready)) if ready else None,
        "tp_count": wins,
        "sl_count": losses,
        "closed_trade_count": closed,
        "winrate_closed": (wins / closed) if closed else None,
        "winrate_wilson_95": _wilson_interval(wins, losses),
        "average_R_filled": (sum(r_values) / len(r_values)) if r_values else None,
        "median_R_filled": median(r_values) if r_values else None,
        "total_R": sum(r_values),
        "profit_factor": (
            positive_r / negative_r
            if negative_r > 0
            else ("Infinity" if positive_r > 0 else None)
        ),
        "median_minutes_to_ready": median(minutes) if minutes else None,
        "ready_signals_per_week": (
            len(ready) / weeks if weeks and weeks > 0 else None
        ),
        "filled_signals_per_week": (
            len(filled) / weeks if weeks and weeks > 0 else None
        ),
        "window_start_utc": window_start.isoformat() if window_start else None,
        "window_end_utc": window_end.isoformat() if window_end else None,
        "window_weeks": weeks,
        "outcomes": dict(sorted(outcome_counts.items())),
        "final_states": dict(sorted(state_counts.items())),
        "top_blockers": blocker_counts.most_common(20),
    }


def run_history_backtest(
    histories: Mapping[str, pd.DataFrame],
    *,
    holdout_fraction: float = 0.30,
) -> dict[str, Any]:
    all_candidates: list[HistoricalWatchCandidate] = []
    coverage: list[dict[str, Any]] = []
    history_by_symbol: dict[str, pd.DataFrame] = {}
    for symbol, raw in sorted(histories.items()):
        normalized = normalize_m5_history(raw, symbol=symbol)
        history_by_symbol[symbol.upper()] = normalized
        candidates, audit = reconstruct_tpo_watch_candidates(
            normalized,
            symbol=symbol,
        )
        all_candidates.extend(candidates)
        coverage.append(audit)

    all_candidates.sort(
        key=lambda item: (item.activated_at_utc, item.candidate_id)
    )
    rows = [
        replay_candidate(
            candidate,
            history_by_symbol[candidate.symbol],
        )
        for candidate in all_candidates
    ]
    return compile_backtest_report(
        candidates=all_candidates,
        rows=rows,
        coverage=coverage,
        holdout_fraction=holdout_fraction,
    )


def compile_backtest_report(
    *,
    candidates: Sequence[HistoricalWatchCandidate],
    rows: Sequence[Mapping[str, Any]],
    coverage: Sequence[Mapping[str, Any]],
    holdout_fraction: float = 0.30,
) -> dict[str, Any]:
    """Compile stable aggregate metrics from streamed per-symbol replays."""

    if not 0.10 <= holdout_fraction <= 0.50:
        raise ValueError("holdout_fraction must be between 0.10 and 0.50")

    all_candidates = sorted(
        candidates,
        key=lambda item: (item.activated_at_utc, item.candidate_id),
    )
    ordered_rows = sorted(
        (dict(row) for row in rows),
        key=lambda row: (
            _as_utc(row["activated_at_utc"]),
            str(row["candidate_id"]),
        ),
    )
    if ordered_rows:
        split_index = max(
            1,
            min(
                len(ordered_rows) - 1,
                int(round(len(ordered_rows) * (1.0 - holdout_fraction))),
            ),
        )
    else:
        split_index = 0
    development_rows = ordered_rows[:split_index]
    holdout_rows = ordered_rows[split_index:]

    by_symbol: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_family: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_direction: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in ordered_rows:
        by_symbol[str(row["symbol"])].append(row)
        by_family[str(row["setup_family"])].append(row)
        by_direction[str(row["direction"])].append(row)

    starts = [
        _as_utc(item["history_first_bar_utc"])
        for item in coverage
        if item.get("history_first_bar_utc")
    ]
    ends = [
        _as_utc(item["history_last_bar_utc"])
        for item in coverage
        if item.get("history_last_bar_utc")
    ]
    window_start = min(starts) if starts else None
    window_end = max(ends) if ends else None
    return {
        "version": LTF_EXECUTION_BACKTEST_VERSION,
        "engine_version": LTF_EXECUTION_STATE_MACHINE_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "mode": "RECONSTRUCTED_TPO_NO_LOOKAHEAD",
        "research_scope": {
            "included_families": [
                "OPEN_TEST_DRIVE",
                "OPEN_REJECTION_REVERSE",
            ],
            "excluded_from_reconstruction": [
                "OPEN_DRIVE",
                "OPEN_AUCTION_BREAKOUT",
                "OPEN_AUCTION_BACK_TO_VALUE",
                "historical_macro_gate",
                "historical_positioning_gate",
                "historical_telegram_delivery",
            ],
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
            "look_ahead_allowed": False,
            "same_bar_entry_and_target_policy": "ambiguous_excluded",
            "same_bar_stop_and_target_policy": "stop_first_conservative",
        },
        "holdout_fraction": holdout_fraction,
        "split_index": split_index,
        "coverage": [dict(item) for item in coverage],
        "metrics": {
            "all": summarize_backtest(
                ordered_rows,
                window_start=window_start,
                window_end=window_end,
            ),
            "development": summarize_backtest(development_rows),
            "holdout": summarize_backtest(holdout_rows),
            "by_symbol": {
                key: summarize_backtest(value)
                for key, value in sorted(by_symbol.items())
            },
            "by_family": {
                key: summarize_backtest(value)
                for key, value in sorted(by_family.items())
            },
            "by_direction": {
                key: summarize_backtest(value)
                for key, value in sorted(by_direction.items())
            },
        },
        "candidates": [candidate.to_dict() for candidate in all_candidates],
        "records": ordered_rows,
    }


__all__ = [
    "HistoricalWatchCandidate",
    "LTF_EXECUTION_BACKTEST_VERSION",
    "ReconstructedProfile",
    "compile_backtest_report",
    "normalize_m5_history",
    "reconstruct_tpo_watch_candidates",
    "replay_candidate",
    "run_history_backtest",
    "summarize_backtest",
]
