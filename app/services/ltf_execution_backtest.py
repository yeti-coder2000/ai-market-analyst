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
    "ltf-execution-v2-backtest-integrity-v1.1-causal-entry-context-costs"
)

MIN_PRIOR_SESSION_M5_BARS = 96
MIN_CURRENT_SESSION_M5_BARS = 24
FIRST_ACTIVITY_MINUTES = 90
DEFAULT_EXECUTION_HORIZON_HOURS = 8
DEFAULT_PREHISTORY_HOURS = 4
DEFAULT_ENTRY_WINDOW_MINUTES = 30
CONTEXT_TPO_BLOCK_MINUTES = 30
MIN_BALANCE_OVERLAP_RATIO = 0.50
SUPPORTED_LIMIT_FILL_POLICIES = {
    "TOUCH",
    "TRADE_THROUGH_HALF_SPREAD",
}


@dataclass(frozen=True, slots=True)
class SessionSpec:
    timezone: str
    open_time: time
    label: str
    max_horizon_hours: int = DEFAULT_EXECUTION_HORIZON_HOURS


@dataclass(frozen=True, slots=True)
class ExecutionCostModel:
    """Explicit price-equivalent research costs for one instrument.

    Defaults are deliberately zero rather than guessed.  A provider/broker
    calibration can supply per-symbol spread and adverse-exit slippage in price
    units plus round-trip commission directly in R.
    """

    spread_price: float = 0.0
    commission_r: float = 0.0
    adverse_exit_slippage_price: float = 0.0
    limit_fill_policy: str = "TOUCH"
    source: str = "UNCONFIGURED_ZERO_COST"

    def __post_init__(self) -> None:
        for name, value in (
            ("spread_price", self.spread_price),
            ("commission_r", self.commission_r),
            (
                "adverse_exit_slippage_price",
                self.adverse_exit_slippage_price,
            ),
        ):
            if not math.isfinite(float(value)) or float(value) < 0:
                raise ValueError(f"{name} must be a finite non-negative number")
        policy = str(self.limit_fill_policy or "").upper()
        if policy not in SUPPORTED_LIMIT_FILL_POLICIES:
            raise ValueError(
                "limit_fill_policy must be one of "
                f"{sorted(SUPPORTED_LIMIT_FILL_POLICIES)}"
            )
        object.__setattr__(self, "limit_fill_policy", policy)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any] | None,
    ) -> "ExecutionCostModel":
        if not value:
            return cls()
        return cls(
            spread_price=float(value.get("spread_price") or 0.0),
            commission_r=float(value.get("commission_r") or 0.0),
            adverse_exit_slippage_price=float(
                value.get("adverse_exit_slippage_price") or 0.0
            ),
            limit_fill_policy=str(
                value.get("limit_fill_policy") or "TOUCH"
            ),
            source=str(value.get("source") or "EXPLICIT_RESEARCH_CONFIG"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class HistoricalContextPoint:
    """Causal auction/watch state known at one completed M5 bar."""

    as_of_utc: datetime
    payload_updates: dict[str, Any]
    cancellation_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "at_utc": self.as_of_utc.isoformat(),
            **self.payload_updates,
            "cancellation_reason": self.cancellation_reason,
        }


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
        profile_context = {
            "profile_id": self.session_id,
            "profile_session_open_utc": self.session_open_utc.isoformat(),
            "profile_session_close_utc": self.session_close_utc.isoformat(),
        }
        return [
            {
                "zone_type": "SESSION_HIGH",
                "role": "STRUCTURAL_INTEREST",
                "price": self.high,
                **profile_context,
            },
            {
                "zone_type": "SESSION_LOW",
                "role": "STRUCTURAL_INTEREST",
                "price": self.low,
                **profile_context,
            },
            {
                "zone_type": "VAH",
                "role": "VALUE_EDGE",
                "price": self.vah,
                **profile_context,
            },
            {
                "zone_type": "VAL",
                "role": "VALUE_EDGE",
                "price": self.val,
                **profile_context,
            },
            {
                "zone_type": "POC",
                "role": "FAIR_VALUE",
                "price": self.poc,
                **profile_context,
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
            "value_edge_tolerance": tolerance,
            "interest_zones": list(zones),
            "cycle_id": activated_at.isoformat(),
            "signal_created_at_utc": activated_at.isoformat(),
            "expires_at_utc": expires_at.isoformat(),
            "profile_reliability": "RECONSTRUCTED_NOT_PARITY_VERIFIED",
            "synthetic_open_confirmed": (
                spec.label
                not in {
                    "LONDON_SYNTHETIC",
                    "TOKYO_SYNTHETIC",
                    "ASIA_SYNTHETIC",
                    "NY_SYNTHETIC",
                }
            ),
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


def _context_blocks(
    history: pd.DataFrame,
    *,
    candidate: HistoricalWatchCandidate,
) -> list[dict[str, Any]]:
    session = _slice(
        history,
        candidate.session_open_utc,
        candidate.expires_at_utc,
    )
    if session.empty:
        return []
    indexed = session.set_index("bar_open_utc")
    blocks = indexed.resample(
        f"{CONTEXT_TPO_BLOCK_MINUTES}min",
        origin=pd.Timestamp(candidate.session_open_utc),
        label="left",
        closed="left",
    ).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
        }
    )
    blocks = blocks.dropna(subset=["open", "high", "low", "close"])
    result: list[dict[str, Any]] = []
    for block_open, row in blocks.iterrows():
        block_end = _as_utc(
            block_open + pd.Timedelta(minutes=CONTEXT_TPO_BLOCK_MINUTES)
        )
        if not candidate.activated_at_utc < block_end <= candidate.expires_at_utc:
            continue
        result.append(
            {
                "block_end_utc": block_end,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
        )
    return result


def _context_relation(
    block: Mapping[str, Any],
    *,
    opened_above: bool,
    edge: float,
    tolerance: float,
) -> str:
    close = float(block["close"])
    edge_buffer = max(0.0, tolerance * 0.25)
    if opened_above:
        if close < edge - edge_buffer:
            return "INSIDE"
        if close > edge + edge_buffer:
            return "OUTSIDE"
    else:
        if close > edge + edge_buffer:
            return "INSIDE"
        if close < edge - edge_buffer:
            return "OUTSIDE"
    return "AT_EDGE"


def _balance_transition(
    blocks: Sequence[Mapping[str, Any]],
    *,
    opened_above: bool,
    edge: float,
    tolerance: float,
) -> str | None:
    if len(blocks) < 2:
        return None
    first, second = blocks[-2:]
    first_range = max(float(first["high"]) - float(first["low"]), tolerance)
    second_range = max(float(second["high"]) - float(second["low"]), tolerance)
    overlap = max(
        0.0,
        min(float(first["high"]), float(second["high"]))
        - max(float(first["low"]), float(second["low"])),
    )
    overlap_ratio = overlap / max(min(first_range, second_range), tolerance)
    close_travel = abs(float(second["close"]) - float(first["close"]))
    balanced = (
        overlap_ratio >= MIN_BALANCE_OVERLAP_RATIO
        and close_travel <= max(tolerance * 2.0, min(first_range, second_range) * 0.50)
    )
    if not balanced:
        return None
    relations = {
        _context_relation(
            block,
            opened_above=opened_above,
            edge=edge,
            tolerance=tolerance,
        )
        for block in (first, second)
    }
    if relations == {"OUTSIDE"}:
        return "OPEN_AUCTION_OUT_OF_RANGE"
    if len(relations) > 1 or "AT_EDGE" in relations:
        return "OPEN_AUCTION"
    return None


def build_dynamic_context_timeline(
    candidate: HistoricalWatchCandidate,
    frame: pd.DataFrame,
) -> list[HistoricalContextPoint]:
    """Reconstruct only causal context changes observable from closed M5 bars.

    This intentionally does not claim Session/Profile parity.  It supplies the
    P0 cancellation layer: price invalidation, failed/accepted value interaction
    and balance transitions that make the original watch stale.
    """

    history = normalize_m5_history(frame, symbol=candidate.symbol)
    bars = _slice(
        history,
        candidate.activated_at_utc,
        candidate.expires_at_utc,
    )
    bars = bars.loc[
        pd.to_datetime(bars["bar_close_utc"], utc=True)
        > pd.Timestamp(candidate.activated_at_utc)
    ]
    if bars.empty:
        return []

    open_location = str(candidate.payload.get("open_location") or "").upper()
    opened_above = "ABOVE" in open_location
    if not opened_above and "BELOW" not in open_location:
        opened_above = candidate.open_price > candidate.previous_vah
    edge = candidate.previous_vah if opened_above else candidate.previous_val
    tick = TICK_SIZE_BY_SYMBOL.get(
        candidate.symbol,
        max(abs(candidate.open_price) * 0.00001, 0.00001),
    )
    tolerance = float(
        candidate.payload.get("value_edge_tolerance") or tick * 3.0
    )
    base_behavior = str(
        candidate.payload.get("current_open_behavior")
        or candidate.payload.get("open_behavior")
        or candidate.setup_family
    ).upper()
    base_value_state = str(
        candidate.payload.get("value_acceptance_state") or "UNKNOWN"
    ).upper()
    base_watch_state = str(
        candidate.payload.get("tpo_watch_state") or "LTF_MODEL_PENDING"
    ).upper()
    profile_reliability = str(
        candidate.payload.get("profile_reliability")
        or "RECONSTRUCTED_NOT_PARITY_VERIFIED"
    )
    synthetic_open_confirmed = candidate.payload.get(
        "synthetic_open_confirmed"
    )
    blocks = _context_blocks(history, candidate=candidate)
    completed_blocks: list[dict[str, Any]] = []
    block_index = 0
    acceptance_count = 0
    rejection_count = 0
    cancellation_reason: str | None = None
    cancelled_at: datetime | None = None
    current_behavior = base_behavior
    value_state = base_value_state
    value_rejection_confirmed = bool(
        candidate.payload.get("value_rejection_confirmed")
    )
    timeline: list[HistoricalContextPoint] = []

    for _, row in bars.iterrows():
        bar_close = _as_utc(row["bar_close_utc"])
        while (
            block_index < len(blocks)
            and blocks[block_index]["block_end_utc"] <= bar_close
        ):
            block = blocks[block_index]
            completed_blocks.append(block)
            relation = _context_relation(
                block,
                opened_above=opened_above,
                edge=edge,
                tolerance=tolerance,
            )
            if relation == "INSIDE":
                acceptance_count += 1
                rejection_count = 0
            elif relation == "OUTSIDE":
                rejection_count += 1
                acceptance_count = 0
            else:
                acceptance_count = 0
                rejection_count = 0
            block_index += 1

        if cancellation_reason is None:
            invalidated = (
                float(row["low"]) <= candidate.test_extreme
                if candidate.direction == "LONG"
                else float(row["high"]) >= candidate.test_extreme
            )
            if invalidated:
                cancellation_reason = "INVALIDATED_BY_CONTEXT_EXTREME"
                cancelled_at = bar_close

        if cancellation_reason is None and candidate.setup_family == "OPEN_TEST_DRIVE":
            if acceptance_count >= 2:
                current_behavior = "OPEN_REJECTION_REVERSE"
                value_state = "ACCEPTED_BACK_INTO_VALUE"
                value_rejection_confirmed = False
                cancellation_reason = "OTD_ACCEPTED_BACK_INTO_VALUE"
                cancelled_at = bar_close
            else:
                balance_behavior = _balance_transition(
                    completed_blocks,
                    opened_above=opened_above,
                    edge=edge,
                    tolerance=tolerance,
                )
                if balance_behavior is not None:
                    current_behavior = balance_behavior
                    value_state = (
                        "BALANCE_OUTSIDE_VALUE"
                        if balance_behavior == "OPEN_AUCTION_OUT_OF_RANGE"
                        else "AUCTION_AROUND_VALUE_EDGE"
                    )
                    value_rejection_confirmed = False
                    cancellation_reason = (
                        "OTD_TRANSITIONED_TO_OAOR"
                        if balance_behavior == "OPEN_AUCTION_OUT_OF_RANGE"
                        else "OTD_TRANSITIONED_TO_OPEN_AUCTION"
                    )
                    cancelled_at = bar_close

        if (
            cancellation_reason is None
            and candidate.setup_family == "OPEN_REJECTION_REVERSE"
        ):
            if rejection_count >= 2:
                current_behavior = "OPEN_TEST_DRIVE_CANDIDATE"
                value_state = "FAILED_ACCEPTANCE_RETURNED_OUTSIDE_VALUE"
                value_rejection_confirmed = True
                cancellation_reason = "ORR_ACCEPTANCE_FAILED_BACK_OUTSIDE_VALUE"
                cancelled_at = bar_close
            else:
                balance_behavior = _balance_transition(
                    completed_blocks,
                    opened_above=opened_above,
                    edge=edge,
                    tolerance=tolerance,
                )
                if balance_behavior is not None:
                    current_behavior = balance_behavior
                    value_state = (
                        "BALANCE_OUTSIDE_VALUE"
                        if balance_behavior == "OPEN_AUCTION_OUT_OF_RANGE"
                        else "AUCTION_AROUND_VALUE_EDGE"
                    )
                    cancellation_reason = "ORR_TRANSITIONED_TO_OPEN_AUCTION"
                    cancelled_at = bar_close

        active = cancellation_reason is None
        transition = (
            f"{base_behavior}->{current_behavior}"
            if current_behavior != base_behavior
            else None
        )
        updates = {
            "current_open_behavior": current_behavior,
            "behavior_transition": transition,
            "value_acceptance_state": value_state,
            "value_acceptance_tpo_count": acceptance_count,
            "value_rejection_tpo_count": rejection_count,
            "value_rejection_confirmed": value_rejection_confirmed,
            "tpo_watch_active": active,
            "tpo_watch_state": base_watch_state if active else "RESEARCH_ONLY",
            "reference_profile_id": candidate.reference_profile_id,
            "profile_reliability": profile_reliability,
            "synthetic_open_confirmed": synthetic_open_confirmed,
            "context_setup_identity": (
                f"{candidate.setup_family}:{candidate.direction}:"
                f"{candidate.reference_profile_id}"
            ),
            "context_cancellation_reason": cancellation_reason,
            "context_cancelled_at_utc": (
                cancelled_at.isoformat() if cancelled_at else None
            ),
        }
        timeline.append(
            HistoricalContextPoint(
                as_of_utc=bar_close,
                payload_updates=updates,
                cancellation_reason=cancellation_reason,
            )
        )
    return timeline


def _context_transition_history(
    candidate: HistoricalWatchCandidate,
    timeline: Sequence[HistoricalContextPoint],
    *,
    until: datetime,
) -> list[dict[str, Any]]:
    events = [
        {
            "at_utc": candidate.activated_at_utc.isoformat(),
            "current_open_behavior": candidate.payload.get(
                "current_open_behavior"
            )
            or candidate.setup_family,
            "value_acceptance_state": candidate.payload.get(
                "value_acceptance_state"
            ),
            "tpo_watch_active": bool(
                candidate.payload.get("tpo_watch_active", True)
            ),
            "reason": "HISTORICAL_WATCH_ACTIVATED",
        }
    ]
    previous_key: tuple[Any, ...] | None = None
    for point in timeline:
        if point.as_of_utc > until:
            break
        updates = point.payload_updates
        key = (
            updates.get("current_open_behavior"),
            updates.get("value_acceptance_state"),
            updates.get("value_acceptance_tpo_count"),
            updates.get("value_rejection_tpo_count"),
            updates.get("tpo_watch_active"),
            point.cancellation_reason,
        )
        if key == previous_key:
            continue
        previous_key = key
        events.append(
            {
                "at_utc": point.as_of_utc.isoformat(),
                "current_open_behavior": updates.get(
                    "current_open_behavior"
                ),
                "behavior_transition": updates.get("behavior_transition"),
                "value_acceptance_state": updates.get(
                    "value_acceptance_state"
                ),
                "value_acceptance_tpo_count": updates.get(
                    "value_acceptance_tpo_count"
                ),
                "value_rejection_tpo_count": updates.get(
                    "value_rejection_tpo_count"
                ),
                "tpo_watch_active": updates.get("tpo_watch_active"),
                "reason": point.cancellation_reason or "CONTEXT_COUNTS_UPDATED",
            }
        )
    return events


def _latest_setup_snapshot(
    store: LTFExecutionStateStore,
) -> dict[str, Any] | None:
    document = store.snapshot()
    setups = document.get("setups")
    if not isinstance(setups, Mapping) or not setups:
        return None
    values = [value for value in setups.values() if isinstance(value, Mapping)]
    if not values:
        return None
    return dict(
        max(
            values,
            key=lambda value: str(value.get("updated_at_utc") or ""),
        )
    )


def _practical_rr_bucket(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NOT_READY"
    if number < 2.0:
        return "LT_2"
    if number < 2.5:
        return "2_TO_2_49"
    if number < 3.0:
        return "2_5_TO_2_99"
    if number < 4.0:
        return "3_TO_3_99"
    return "GE_4"


def _target_diagnostics(
    candidate: HistoricalWatchCandidate,
    history: pd.DataFrame,
    *,
    ready_at: datetime,
    target: float,
    target_zone_type: Any,
) -> dict[str, Any]:
    tick = TICK_SIZE_BY_SYMBOL.get(
        candidate.symbol,
        max(abs(target) * 0.00001, 0.00001),
    )
    selected: Mapping[str, Any] | None = None
    for zone in candidate.interest_zones:
        try:
            price_matches = abs(float(zone.get("price")) - target) <= tick
        except (TypeError, ValueError):
            continue
        type_matches = (
            not target_zone_type
            or str(zone.get("zone_type") or "").upper()
            == str(target_zone_type).upper()
        )
        if price_matches and type_matches:
            selected = zone
            break

    profile_close = (
        _as_utc(selected.get("profile_session_close_utc"))
        if selected is not None
        else None
    )
    age_days = (
        max(0.0, (ready_at - profile_close).total_seconds() / 86400.0)
        if profile_close is not None
        else None
    )
    if profile_close is None:
        freshness = "UNKNOWN_PROFILE_AGE"
    else:
        prior = history.loc[
            (
                pd.to_datetime(history["bar_close_utc"], utc=True)
                > pd.Timestamp(profile_close)
            )
            & (
                pd.to_datetime(history["bar_close_utc"], utc=True)
                <= pd.Timestamp(ready_at)
            )
        ]
        touched = (
            (prior["low"] <= target + tick)
            & (prior["high"] >= target - tick)
        )
        freshness = (
            "STALE_PREVIOUSLY_TRADED"
            if bool(touched.any())
            else "FRESH_UNTOUCHED_SINCE_PROFILE"
        )
    return {
        "target_zone_profile_id": (
            selected.get("profile_id") if selected is not None else None
        ),
        "target_zone_age_days": (
            round(age_days, 6) if age_days is not None else None
        ),
        "target_freshness": freshness,
    }


def _finalize_execution_outcome(
    *,
    outcome: str,
    gross_r: float | None,
    filled_at: datetime | None,
    resolved_at: datetime,
    risk: float,
    cost_model: ExecutionCostModel,
    mfe_price: float | None = None,
    mae_price: float | None = None,
    cancellation_reason: str | None = None,
) -> dict[str, Any]:
    filled = filled_at is not None
    spread_cost_r = (
        cost_model.spread_price / risk if filled and risk > 0 else 0.0
    )
    commission_cost_r = cost_model.commission_r if filled else 0.0
    slippage_applies = bool(
        filled
        and (
            outcome.startswith("SL_HIT")
            or outcome == "EXPIRED_AFTER_ENTRY"
        )
    )
    adverse_slippage_cost_r = (
        cost_model.adverse_exit_slippage_price / risk
        if slippage_applies and risk > 0
        else 0.0
    )
    total_cost_r = (
        spread_cost_r + commission_cost_r + adverse_slippage_cost_r
    )
    net_r = gross_r - total_cost_r if gross_r is not None else None
    return {
        "outcome": outcome,
        # Backwards-compatible alias. Integrity v1.1 reports gross/net
        # separately and uses net R for its primary expectancy metrics.
        "realized_R": gross_r,
        "gross_R": gross_r,
        "net_R": net_r,
        "spread_cost_R": spread_cost_r,
        "commission_cost_R": commission_cost_r,
        "adverse_slippage_cost_R": adverse_slippage_cost_r,
        "total_cost_R": total_cost_r,
        "filled_at_utc": filled_at.isoformat() if filled_at else None,
        "resolved_at_utc": resolved_at.isoformat(),
        "mfe_R": (
            max(0.0, float(mfe_price)) / risk
            if mfe_price is not None and risk > 0
            else None
        ),
        "mae_R": (
            max(0.0, float(mae_price)) / risk
            if mae_price is not None and risk > 0
            else None
        ),
        "cancellation_reason": cancellation_reason,
        "execution_cost_model": cost_model.to_dict(),
    }


def _simulate_limit_outcome(
    bars: pd.DataFrame,
    *,
    direction: str,
    ready_at: datetime,
    entry_window_expires_at: datetime,
    trade_resolution_expires_at: datetime,
    entry: float,
    stop: float,
    target: float,
    cost_model: ExecutionCostModel | Mapping[str, Any] | None = None,
    context_timeline: Sequence[HistoricalContextPoint] = (),
) -> dict[str, Any]:
    risk = abs(entry - stop)
    effective_costs = (
        cost_model
        if isinstance(cost_model, ExecutionCostModel)
        else ExecutionCostModel.from_mapping(cost_model)
    )
    if risk <= 0 or not math.isfinite(risk):
        return _finalize_execution_outcome(
            outcome="INVALID_GEOMETRY",
            gross_r=None,
            filled_at=None,
            resolved_at=ready_at,
            risk=max(risk, 0.0),
            cost_model=effective_costs,
        )
    entry_expiry = min(
        entry_window_expires_at,
        trade_resolution_expires_at,
    )
    forward = bars.loc[
        (
            pd.to_datetime(bars["bar_close_utc"], utc=True)
            > pd.Timestamp(ready_at)
        )
        & (
            pd.to_datetime(bars["bar_close_utc"], utc=True)
            <= pd.Timestamp(trade_resolution_expires_at)
        )
    ]
    filled_at: datetime | None = None
    last_close = entry
    mfe_price: float | None = None
    mae_price: float | None = None
    points = sorted(context_timeline, key=lambda point: point.as_of_utc)
    context_index = 0
    active_context: HistoricalContextPoint | None = None

    for _, row in forward.iterrows():
        bar_close = _as_utc(row["bar_close_utc"])
        if filled_at is None and bar_close > entry_expiry:
            return _finalize_execution_outcome(
                outcome="ENTRY_WINDOW_EXPIRED_UNFILLED",
                gross_r=0.0,
                filled_at=None,
                resolved_at=entry_expiry,
                risk=risk,
                cost_model=effective_costs,
                cancellation_reason="ENTRY_WINDOW_EXPIRED",
            )

        while (
            context_index < len(points)
            and points[context_index].as_of_utc <= bar_close
        ):
            active_context = points[context_index]
            context_index += 1
        if (
            filled_at is None
            and active_context is not None
            and active_context.cancellation_reason
        ):
            reason = str(active_context.cancellation_reason)
            outcome = (
                "INVALIDATED_BEFORE_FILL"
                if reason == "INVALIDATED_BY_CONTEXT_EXTREME"
                else "CONTEXT_CANCELLED_BEFORE_FILL"
            )
            return _finalize_execution_outcome(
                outcome=outcome,
                gross_r=0.0,
                filled_at=None,
                resolved_at=active_context.as_of_utc,
                risk=risk,
                cost_model=effective_costs,
                cancellation_reason=reason,
            )

        high = float(row["high"])
        low = float(row["low"])
        last_close = float(row["close"])
        if effective_costs.limit_fill_policy == "TRADE_THROUGH_HALF_SPREAD":
            half_spread = effective_costs.spread_price * 0.50
            touches_entry = (
                low <= entry - half_spread
                if direction == "LONG"
                else high >= entry + half_spread
            )
        else:
            touches_entry = low <= entry <= high
        touches_stop = low <= stop if direction == "LONG" else high >= stop
        touches_target = high >= target if direction == "LONG" else low <= target
        filled_on_this_bar = False

        if filled_at is None:
            if touches_stop and not touches_entry:
                return _finalize_execution_outcome(
                    outcome="INVALIDATED_BEFORE_FILL",
                    gross_r=0.0,
                    filled_at=None,
                    resolved_at=bar_close,
                    risk=risk,
                    cost_model=effective_costs,
                    cancellation_reason="STOP_OR_CONTEXT_INVALIDATION_BEFORE_FILL",
                )
            if touches_entry and touches_target:
                return _finalize_execution_outcome(
                    outcome="AMBIGUOUS_ENTRY_AND_TARGET_SAME_BAR",
                    gross_r=None,
                    filled_at=None,
                    resolved_at=bar_close,
                    risk=risk,
                    cost_model=effective_costs,
                )
            if not touches_entry:
                if touches_target:
                    return _finalize_execution_outcome(
                        outcome="MISSED_TARGET_BEFORE_ENTRY",
                        gross_r=0.0,
                        filled_at=None,
                        resolved_at=bar_close,
                        risk=risk,
                        cost_model=effective_costs,
                    )
                continue
            filled_at = bar_close
            filled_on_this_bar = True

        if direction == "LONG":
            favorable = max(0.0, high - entry)
            adverse = max(0.0, entry - low)
        else:
            favorable = max(0.0, entry - low)
            adverse = max(0.0, high - entry)
        mfe_price = max(mfe_price or 0.0, favorable)
        mae_price = max(mae_price or 0.0, adverse)

        if touches_stop and touches_target:
            return _finalize_execution_outcome(
                outcome="SL_HIT_AMBIGUOUS_BAR_CONSERVATIVE",
                gross_r=-1.0,
                filled_at=filled_at,
                resolved_at=bar_close,
                risk=risk,
                cost_model=effective_costs,
                mfe_price=mfe_price,
                mae_price=mae_price,
            )
        if touches_stop:
            return _finalize_execution_outcome(
                outcome=(
                    "SL_HIT_AMBIGUOUS_ENTRY_BAR_CONSERVATIVE"
                    if filled_on_this_bar
                    else "SL_HIT"
                ),
                gross_r=-1.0,
                filled_at=filled_at,
                resolved_at=bar_close,
                risk=risk,
                cost_model=effective_costs,
                mfe_price=mfe_price,
                mae_price=mae_price,
            )
        if touches_target:
            reward_r = abs(target - entry) / risk
            return _finalize_execution_outcome(
                outcome="TP_HIT",
                gross_r=reward_r,
                filled_at=filled_at,
                resolved_at=bar_close,
                risk=risk,
                cost_model=effective_costs,
                mfe_price=mfe_price,
                mae_price=mae_price,
            )

    if filled_at is None:
        return _finalize_execution_outcome(
            outcome="ENTRY_WINDOW_EXPIRED_UNFILLED",
            gross_r=0.0,
            filled_at=None,
            resolved_at=entry_expiry,
            risk=risk,
            cost_model=effective_costs,
            cancellation_reason="ENTRY_WINDOW_EXPIRED",
        )
    mark_r = (
        (last_close - entry) / risk
        if direction == "LONG"
        else (entry - last_close) / risk
    )
    return _finalize_execution_outcome(
        outcome="EXPIRED_AFTER_ENTRY",
        gross_r=max(
            -1.0,
            min(mark_r, abs(target - entry) / risk),
        ),
        filled_at=filled_at,
        resolved_at=trade_resolution_expires_at,
        risk=risk,
        cost_model=effective_costs,
        mfe_price=mfe_price,
        mae_price=mae_price,
    )


def replay_candidate(
    candidate: HistoricalWatchCandidate,
    frame: pd.DataFrame,
    *,
    cost_model: ExecutionCostModel | Mapping[str, Any] | None = None,
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
    context_timeline = build_dynamic_context_timeline(candidate, history)
    context_by_close = {
        point.as_of_utc: point for point in context_timeline
    }
    store = LTFExecutionStateStore(path=None)
    initial_payload = dict(candidate.payload)
    result = store.evaluate(
        initial_payload,
        df_5m=replay,
        as_of=candidate.activated_at_utc,
        persist=False,
    )
    ready_result: dict[str, Any] | None = None
    ready_setup_snapshot: dict[str, Any] | None = None
    evaluation_count = 1

    future_closes = sorted(
        {
            _as_utc(value)
            for value in replay["bar_close_utc"]
            if candidate.activated_at_utc < _as_utc(value) <= candidate.expires_at_utc
        }
    )
    for bar_close in future_closes:
        point = context_by_close.get(bar_close)
        dynamic_payload = dict(candidate.payload)
        if point is not None:
            dynamic_payload.update(point.payload_updates)
        result = store.evaluate(
            dynamic_payload,
            df_5m=replay,
            as_of=bar_close,
            persist=False,
        )
        evaluation_count += 1
        if result.get("ltf_execution_v2_state") == STATE_ENTRY_READY:
            ready_result = result
            ready_setup_snapshot = _latest_setup_snapshot(store)
            break

    final_setup_snapshot = (
        ready_setup_snapshot or _latest_setup_snapshot(store) or {}
    )
    terminal_reason = final_setup_snapshot.get("terminal_reason")
    first_cancellation_point = next(
        (
            point
            for point in context_timeline
            if point.cancellation_reason
        ),
        None,
    )
    context_cancellation_reason = (
        first_cancellation_point.cancellation_reason
        if first_cancellation_point is not None
        else None
    )
    if context_cancellation_reason == "INVALIDATED_BY_CONTEXT_EXTREME":
        no_ready_outcome = "INVALIDATED_BEFORE_READY"
    elif context_cancellation_reason:
        no_ready_outcome = "CONTEXT_CANCELLED_BEFORE_READY"
    else:
        no_ready_outcome = "NO_ENTRY_READY"
    if (
        context_cancellation_reason
        and first_cancellation_point is not None
    ):
        no_ready_resolved_at = first_cancellation_point.as_of_utc
    else:
        no_ready_resolved_at = candidate.expires_at_utc
    transition_history = (
        final_setup_snapshot.get("transition_history")
        or result.get("ltf_execution_v2_transition_history")
        or []
    )
    row: dict[str, Any] = {
        "backtest_version": LTF_EXECUTION_BACKTEST_VERSION,
        "engine_version": LTF_EXECUTION_STATE_MACHINE_VERSION,
        "candidate_id": candidate.candidate_id,
        "symbol": candidate.symbol,
        "setup_family": candidate.setup_family,
        "direction": candidate.direction,
        "htf_bias": candidate.htf_bias,
        "htf_alignment_state": candidate.payload.get("signal_alignment"),
        "profile_reliability": candidate.payload.get(
            "profile_reliability"
        )
        or "RECONSTRUCTED_NOT_PARITY_VERIFIED",
        "synthetic_open_confirmed": candidate.payload.get(
            "synthetic_open_confirmed"
        ),
        "open_location": candidate.payload.get("open_location"),
        "initial_open_behavior": candidate.payload.get(
            "initial_open_behavior"
        )
        or candidate.payload.get("open_behavior")
        or candidate.setup_family,
        "activated_current_open_behavior": candidate.payload.get(
            "current_open_behavior"
        )
        or candidate.setup_family,
        "activated_at_utc": candidate.activated_at_utc.isoformat(),
        "expires_at_utc": candidate.expires_at_utc.isoformat(),
        "trade_resolution_expires_at_utc": candidate.expires_at_utc.isoformat(),
        "entry_window_expires_at_utc": None,
        "evaluation_count": evaluation_count,
        "final_ltf_state": final_setup_snapshot.get("state")
        or result.get("ltf_execution_v2_state"),
        "final_ltf_outcome": result.get("ltf_model_outcome"),
        "ready": ready_result is not None,
        "ready_at_utc": None,
        "minutes_to_ready": None,
        "minutes_ready_to_fill": None,
        "entry_reference_price": None,
        "invalidation_reference_price": None,
        "target_reference_price": None,
        "stop_distance": None,
        "atr_at_bos": final_setup_snapshot.get("atr_at_bos"),
        "stop_distance_ATR": None,
        "retest_depth_ATR": None,
        "trigger_extension_R": None,
        "risk_reward_ratio": None,
        "practical_rr_bucket": "NOT_READY",
        "target_zone_type": None,
        "target_zone_role": None,
        "target_zone_profile_id": None,
        "target_zone_age_days": None,
        "target_freshness": None,
        "outcome": no_ready_outcome,
        "realized_R": 0.0,
        "gross_R": 0.0,
        "net_R": 0.0,
        "spread_cost_R": 0.0,
        "commission_cost_R": 0.0,
        "adverse_slippage_cost_R": 0.0,
        "total_cost_R": 0.0,
        "mfe_R": None,
        "mae_R": None,
        "filled_at_utc": None,
        "resolved_at_utc": no_ready_resolved_at.isoformat(),
        "cancellation_reason": context_cancellation_reason
        or terminal_reason,
        "context_transition_history": _context_transition_history(
            candidate,
            context_timeline,
            until=no_ready_resolved_at,
        ),
        "behavior_transition_count_before_ready": 0,
        "behavior_transition_count_before_fill": 0,
        "execution_cost_model": (
            cost_model.to_dict()
            if isinstance(cost_model, ExecutionCostModel)
            else ExecutionCostModel.from_mapping(cost_model).to_dict()
        ),
        "blockers": result.get("ltf_model_blockers") or [],
        "transition_history": transition_history,
    }
    if ready_result is None:
        if terminal_reason and str(terminal_reason).upper() not in row["blockers"]:
            row["blockers"] = [
                *row["blockers"],
                str(terminal_reason).upper(),
            ]
        return row

    ready_at = _as_utc(ready_result["ltf_execution_v2_entry_ready_at_utc"])
    entry_expiry_value = ready_result.get("entry_window_expires_at_utc")
    entry_window_expires_at = (
        _as_utc(entry_expiry_value)
        if entry_expiry_value
        else ready_at + timedelta(minutes=DEFAULT_ENTRY_WINDOW_MINUTES)
    )
    entry = float(ready_result["entry_reference_price"])
    stop = float(ready_result["invalidation_reference_price"])
    target = float(ready_result["target_reference_price"])
    outcome = _simulate_limit_outcome(
        replay,
        direction=candidate.direction,
        ready_at=ready_at,
        entry_window_expires_at=entry_window_expires_at,
        trade_resolution_expires_at=candidate.expires_at_utc,
        entry=entry,
        stop=stop,
        target=target,
        cost_model=cost_model,
        context_timeline=context_timeline,
    )
    resolved_value = outcome.get("resolved_at_utc")
    resolved_at = (
        _as_utc(resolved_value)
        if resolved_value
        else candidate.expires_at_utc
    )
    filled_value = outcome.get("filled_at_utc")
    filled_at = _as_utc(filled_value) if filled_value else None
    context_history = _context_transition_history(
        candidate,
        context_timeline,
        until=resolved_at,
    )
    transitions_before_ready = sum(
        1
        for event in context_history
        if event.get("behavior_transition")
        and _as_utc(event["at_utc"]) <= ready_at
    )
    transitions_before_fill = sum(
        1
        for event in context_history
        if event.get("behavior_transition")
        and filled_at is not None
        and _as_utc(event["at_utc"]) <= filled_at
    )
    ready_setup_snapshot = ready_setup_snapshot or {}
    atr_at_bos = ready_setup_snapshot.get("atr_at_bos")
    try:
        atr_value = float(atr_at_bos)
    except (TypeError, ValueError):
        atr_value = 0.0
    stop_distance = abs(entry - stop)
    bos_level = ready_setup_snapshot.get("bos_level")
    retest_extreme = ready_setup_snapshot.get("retest_extreme")
    retest_depth_atr = None
    if atr_value > 0 and bos_level is not None and retest_extreme is not None:
        retest_depth_atr = abs(
            float(bos_level) - float(retest_extreme)
        ) / atr_value
    target_details = _target_diagnostics(
        candidate,
        history,
        ready_at=ready_at,
        target=target,
        target_zone_type=ready_result.get("target_zone_type"),
    )
    practical_rr = ready_result.get("practical_rr")
    if practical_rr is None:
        practical_rr = ready_result.get("risk_reward_ratio")
    row.update(
        {
            "ready_at_utc": ready_at.isoformat(),
            "entry_window_expires_at_utc": entry_window_expires_at.isoformat(),
            "minutes_to_ready": round(
                (ready_at - candidate.activated_at_utc).total_seconds() / 60.0,
                4,
            ),
            "minutes_ready_to_fill": (
                round((filled_at - ready_at).total_seconds() / 60.0, 4)
                if filled_at is not None
                else None
            ),
            "entry_reference_price": entry,
            "invalidation_reference_price": stop,
            "target_reference_price": target,
            "stop_distance": stop_distance,
            "atr_at_bos": atr_at_bos,
            "stop_distance_ATR": (
                stop_distance / atr_value if atr_value > 0 else None
            ),
            "retest_depth_ATR": retest_depth_atr,
            "trigger_extension_R": ready_result.get("already_moved_R"),
            "risk_reward_ratio": float(ready_result["risk_reward_ratio"]),
            "practical_rr_bucket": _practical_rr_bucket(practical_rr),
            "target_zone_type": ready_result.get("target_zone_type"),
            "target_zone_role": ready_result.get("target_zone_role"),
            **target_details,
            "outcome": outcome["outcome"],
            "realized_R": outcome["realized_R"],
            "gross_R": outcome["gross_R"],
            "net_R": outcome["net_R"],
            "spread_cost_R": outcome["spread_cost_R"],
            "commission_cost_R": outcome["commission_cost_R"],
            "adverse_slippage_cost_R": outcome[
                "adverse_slippage_cost_R"
            ],
            "total_cost_R": outcome["total_cost_R"],
            "mfe_R": outcome["mfe_R"],
            "mae_R": outcome["mae_R"],
            "filled_at_utc": outcome.get("filled_at_utc"),
            "resolved_at_utc": outcome.get("resolved_at_utc"),
            "cancellation_reason": outcome.get("cancellation_reason"),
            "context_transition_history": context_history,
            "behavior_transition_count_before_ready": transitions_before_ready,
            "behavior_transition_count_before_fill": transitions_before_fill,
            "execution_cost_model": outcome["execution_cost_model"],
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
    gross_r_values = [
        float(
            row["gross_R"]
            if row.get("gross_R") is not None
            else row["realized_R"]
        )
        for row in filled
        if row.get("gross_R") is not None
        or row.get("realized_R") is not None
    ]
    net_r_values = [
        float(
            row["net_R"]
            if row.get("net_R") is not None
            else (
                row["gross_R"]
                if row.get("gross_R") is not None
                else row["realized_R"]
            )
        )
        for row in filled
        if row.get("net_R") is not None
        or row.get("gross_R") is not None
        or row.get("realized_R") is not None
    ]
    gross_positive_r = sum(value for value in gross_r_values if value > 0)
    gross_negative_r = abs(
        sum(value for value in gross_r_values if value < 0)
    )
    net_positive_r = sum(value for value in net_r_values if value > 0)
    net_negative_r = abs(sum(value for value in net_r_values if value < 0))
    minutes = [
        float(row["minutes_to_ready"])
        for row in ready
        if row.get("minutes_to_ready") is not None
    ]
    fill_minutes = [
        float(row["minutes_ready_to_fill"])
        for row in filled
        if row.get("minutes_ready_to_fill") is not None
    ]
    mfe_values = [
        float(row["mfe_R"])
        for row in filled
        if row.get("mfe_R") is not None
    ]
    mae_values = [
        float(row["mae_R"])
        for row in filled
        if row.get("mae_R") is not None
    ]
    total_cost_r = sum(
        float(row.get("total_cost_R") or 0.0) for row in filled
    )
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
        "metric_basis": "NET_R",
        "average_gross_R_filled": (
            sum(gross_r_values) / len(gross_r_values)
            if gross_r_values
            else None
        ),
        "median_gross_R_filled": (
            median(gross_r_values) if gross_r_values else None
        ),
        "gross_total_R": sum(gross_r_values),
        "gross_profit_factor": (
            gross_positive_r / gross_negative_r
            if gross_negative_r > 0
            else ("Infinity" if gross_positive_r > 0 else None)
        ),
        "average_net_R_filled": (
            sum(net_r_values) / len(net_r_values)
            if net_r_values
            else None
        ),
        "median_net_R_filled": (
            median(net_r_values) if net_r_values else None
        ),
        "net_total_R": sum(net_r_values),
        "net_profit_factor": (
            net_positive_r / net_negative_r
            if net_negative_r > 0
            else ("Infinity" if net_positive_r > 0 else None)
        ),
        "total_cost_R": total_cost_r,
        # Compatibility aliases now intentionally use net performance.
        "average_R_filled": (
            sum(net_r_values) / len(net_r_values)
            if net_r_values
            else None
        ),
        "median_R_filled": (
            median(net_r_values) if net_r_values else None
        ),
        "total_R": sum(net_r_values),
        "profit_factor": (
            net_positive_r / net_negative_r
            if net_negative_r > 0
            else ("Infinity" if net_positive_r > 0 else None)
        ),
        "median_minutes_to_ready": median(minutes) if minutes else None,
        "median_minutes_ready_to_fill": (
            median(fill_minutes) if fill_minutes else None
        ),
        "median_MFE_R": median(mfe_values) if mfe_values else None,
        "median_MAE_R": median(mae_values) if mae_values else None,
        "entry_window_expired_unfilled_count": outcome_counts.get(
            "ENTRY_WINDOW_EXPIRED_UNFILLED",
            0,
        ),
        "context_cancelled_before_fill_count": outcome_counts.get(
            "CONTEXT_CANCELLED_BEFORE_FILL",
            0,
        ),
        "invalidated_before_fill_count": outcome_counts.get(
            "INVALIDATED_BEFORE_FILL",
            0,
        ),
        "context_cancelled_before_ready_count": outcome_counts.get(
            "CONTEXT_CANCELLED_BEFORE_READY",
            0,
        ),
        "invalidated_before_ready_count": outcome_counts.get(
            "INVALIDATED_BEFORE_READY",
            0,
        ),
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
    cost_models: Mapping[
        str,
        ExecutionCostModel | Mapping[str, Any],
    ] | None = None,
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
            cost_model=(cost_models or {}).get(candidate.symbol),
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
    by_symbol_family: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_rr_bucket: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    execution_cost_models: dict[str, dict[str, Any]] = {}
    for row in ordered_rows:
        symbol = str(row["symbol"])
        family = str(row["setup_family"])
        by_symbol[symbol].append(row)
        by_family[family].append(row)
        by_direction[str(row["direction"])].append(row)
        by_symbol_family[f"{symbol}|{family}"].append(row)
        by_rr_bucket[
            str(row.get("practical_rr_bucket") or "NOT_READY")
        ].append(row)
        model = row.get("execution_cost_model")
        if symbol not in execution_cost_models and isinstance(model, Mapping):
            execution_cost_models[symbol] = dict(model)

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
        "execution_integrity": {
            "entry_fill_deadline": "entry_window_expires_at_utc",
            "trade_resolution_deadline": "trade_resolution_expires_at_utc",
            "deadlines_are_separate": True,
            "dynamic_context_timeline": True,
            "unfilled_limit_cancelled_on_context_change": True,
            "unfilled_limit_cancelled_on_price_invalidation": True,
            "gross_and_net_results": True,
            "primary_expectancy_basis": "NET_R",
            "cost_model_status": (
                "EXPLICIT_PER_SYMBOL_CONFIG"
                if any(
                    model.get("source") != "UNCONFIGURED_ZERO_COST"
                    for model in execution_cost_models.values()
                )
                else "UNCONFIGURED_ZERO_COST"
            ),
            "mfe_mae_same_fill_bar_policy": "whole_bar_included",
        },
        "execution_cost_models": dict(sorted(execution_cost_models.items())),
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
            "same_bar_entry_and_invalidation_policy": (
                "context_cancel_before_fill_conservative"
            ),
            "session_profile_parity": "DEFERRED_TO_P1",
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
            "by_symbol_family": {
                key: summarize_backtest(value)
                for key, value in sorted(by_symbol_family.items())
            },
            "by_practical_rr_bucket": {
                key: summarize_backtest(value)
                for key, value in sorted(by_rr_bucket.items())
            },
        },
        "candidates": [candidate.to_dict() for candidate in all_candidates],
        "records": ordered_rows,
    }


__all__ = [
    "ExecutionCostModel",
    "HistoricalWatchCandidate",
    "HistoricalContextPoint",
    "LTF_EXECUTION_BACKTEST_VERSION",
    "ReconstructedProfile",
    "build_dynamic_context_timeline",
    "compile_backtest_report",
    "normalize_m5_history",
    "reconstruct_tpo_watch_candidates",
    "replay_candidate",
    "run_history_backtest",
    "summarize_backtest",
]
