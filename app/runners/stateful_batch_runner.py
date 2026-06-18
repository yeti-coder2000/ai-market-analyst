from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
import json
import os
from pathlib import Path
import time
from typing import Any, Optional

import pandas as pd

from app.core.enums import Instrument, Timeframe
from app.core.instrument_batches import get_batch_symbols
from app.core.logger import bind_logger, get_logger
from app.core.settings import settings
from app.providers.twelvedata_client import TwelveDataClient, TwelveDataClientConfig
from app.providers.twelvedata_provider_adapter import AdapterConfig, TwelveDataProviderAdapter
from app.scenarios.behavioral import (
    classify_market_phase,
    extract_context_htf_bias,
    extract_context_market_state,
    extract_context_price,
    infer_alignment_score,
    infer_behavioral_scenario,
    infer_missing_conditions,
    infer_next_expected_event,
)
from app.scenarios.execution import build_execution_plan
from app.services.consistency_checker import check_consistency
from app.services.loader import MarketDataLoader
from app.services.radar_journal import (
    write_cycle_finished,
    write_cycle_started,
    write_instrument_analyzed,
    write_instrument_snapshot,
    write_signal_candidate_detected,
    write_signal_registered,
    write_signal_resolved,
    write_signal_updated,
)
from app.services.signal_quality_engine import enrich_payload_with_quality
from app.services.tpo_watch_bridge import enrich_payload_with_tpo_watch
from app.services.tpo_ltf_model_detector import enrich_payload_with_ltf_model
from app.services.post_news_continuation_detector import apply_post_news_continuation
from app.services.signal_tracker import SignalTracker, SignalTrackerResult
from app.services.telegram_formatter import format_signal_message
from app.services.telegram_notifier import build_telegram_notifier
from app.services.telegram_alert_store import record_telegram_alert
from app.storage.cache_store import ParquetCache

logger = get_logger(__name__, component="stateful_batch_runner")

RUNNER_VERSION = "1.5.4-post-news-otd-bridge"

# Telegram is a trade-alert channel, not a reconnaissance feed.
# WATCH / EDGE_FORMING / SCENARIO_FORMING must be persisted to journal/statistics,
# but must not reach Telegram.
TELEGRAM_MIN_RR = 2.0
TELEGRAM_MAX_RR = 10.0
TELEGRAM_MIN_CONFIDENCE = 0.60
TELEGRAM_LATE_SIGNAL_THRESHOLD_R = float(os.getenv("TELEGRAM_LATE_SIGNAL_THRESHOLD_R", "0.5"))
TELEGRAM_HARD_LATE_SIGNAL_THRESHOLD_R = float(os.getenv("TELEGRAM_HARD_LATE_SIGNAL_THRESHOLD_R", "0.7"))
EXECUTION_TIMING_GUARD_VERSION = "execution-timing-guard-v1.0-runner-hard-gate"

ENTRY_TIMING_FIELD_KEYS = (
    "current_price",
    "entry_distance",
    "entry_distance_R",
    "already_moved_R",
    "entry_timing_status",
    "wait_retest_only",
    "late_signal_reason",
    "entry_retest_required",
    "execution_timing_guard_version",
)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


# =============================================================================
# GUARANTEED HISTORY WRITE LAYER (PERSISTENT DISK READY)
# =============================================================================

RUNTIME_DIR = settings.runtime_dir
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

JOURNAL_FALLBACK_PATH = RUNTIME_DIR / "radar_journal.ndjson"
SNAPSHOT_FALLBACK_PATH = RUNTIME_DIR / "radar_snapshot_v2.ndjson"

# TPO is intentionally NOT calculated inside the live worker.
# Live worker only reads precomputed auction context from this store.
# This prevents Render OOM spikes and keeps signal production stable.
TPO_STORE_PATH = RUNTIME_DIR / "tpo" / "tpo_latest.json"
TPO_MAX_STALE_MINUTES = int(os.getenv("TPO_MAX_STALE_MINUTES", "240"))
TPO_SIGNAL_GATE_ENABLED = _env_bool("TPO_SIGNAL_GATE_ENABLED", True)

# Guard against false MARKET_CLOSED propagation for NY-focused instruments.
# MARKET_CLOSED is a session/calendar state; STALE_DATA is a provider/freshness state.
# The runner must not mark NAS100/SPX500/UKOIL as MARKET_CLOSED during NY / US /
# post-news context only because the offline TPO store or provider freshness is stale.
RUNNER_NY_MARKET_STATUS_GUARD_ENABLED = _env_bool(
    "RUNNER_NY_MARKET_STATUS_GUARD_ENABLED",
    True,
)
RUNNER_NY_MARKET_STATUS_GUARD_SYMBOLS = {"NAS100", "SPX500", "UKOIL"}
RUNNER_NY_MARKET_STATUS_GUARD_TOKENS = (
    "NY",
    "NY_1H",
    "NEW_YORK",
    "NEW YORK",
    "NEW_YORK_CASH",
    "US_SESSION",
    "US CASH",
    "POST_NEWS",
    "AFTER_NEWS",
)
RUNNER_CLOSED_MARKET_STATUSES = {"MARKET_CLOSED", "CLOSED", "MARKET_CLOSED_AND_STALE"}


def safe_append_ndjson(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        f.flush()


# =============================================================================
# CONFIG
# =============================================================================

DEFAULT_TIMEFRAMES_BY_SYMBOL: dict[Instrument, list[Timeframe]] = {
    Instrument.XAUUSD: [Timeframe.M15, Timeframe.M30, Timeframe.H1, Timeframe.H4, Timeframe.D1],
    Instrument.EURUSD: [Timeframe.M15, Timeframe.M30, Timeframe.H1, Timeframe.H4, Timeframe.D1],
    Instrument.GBPUSD: [Timeframe.M15, Timeframe.M30, Timeframe.H1, Timeframe.H4, Timeframe.D1],
    Instrument.BTCUSD: [Timeframe.M15, Timeframe.M30, Timeframe.H1, Timeframe.H4, Timeframe.D1],
    Instrument.ETHUSD: [Timeframe.M15, Timeframe.M30, Timeframe.H1, Timeframe.H4, Timeframe.D1],
    Instrument.UKOIL: [Timeframe.M15, Timeframe.M30, Timeframe.H1, Timeframe.H4, Timeframe.D1],
    Instrument.GER40: [Timeframe.M15, Timeframe.M30, Timeframe.H1, Timeframe.H4, Timeframe.D1],
    Instrument.NAS100: [Timeframe.M15, Timeframe.M30, Timeframe.H1, Timeframe.H4, Timeframe.D1],
    Instrument.SPX500: [Timeframe.M15, Timeframe.M30, Timeframe.H1, Timeframe.H4, Timeframe.D1],
}

DEFAULT_INSTRUMENT_PROFILES: list[dict[str, Any]] = [
    {"symbol": Instrument.XAUUSD, "priority": 1},
    {"symbol": Instrument.BTCUSD, "priority": 2},
    {"symbol": Instrument.ETHUSD, "priority": 2},
    {"symbol": Instrument.EURUSD, "priority": 2},
    {"symbol": Instrument.GBPUSD, "priority": 2},
]


# =============================================================================
# PASSIVE AUCTION / TPO TELEMETRY LIMITS
# =============================================================================
#
# IMPORTANT:
# profile_engine approximates a volume profile by distributing candle volume
# across price bins between candle low/high. If we feed thousands of 15m bars
# with tiny tick sizes, memory can spike on Render. Therefore TPO telemetry is
# intentionally coarse and bounded. This is a context/statistics layer, not an
# execution price ladder.

AUCTION_MAX_BARS = int(os.getenv("AUCTION_MAX_BARS", "672"))  # ~7 days of 15m bars
AUCTION_DEFAULT_TICK_SIZE = 0.001

AUCTION_TICK_SIZE_BY_SYMBOL: dict[str, float] = {
    # Coarse bins by design: stable telemetry > perfect profile precision.
    "XAUUSD": 5.0,
    "BTCUSD": 100.0,
    "ETHUSD": 10.0,
    "EURUSD": 0.001,
    "GBPUSD": 0.001,
    "USDJPY": 0.10,
    "USDCHF": 0.001,
    "USDCAD": 0.001,
    "AUDUSD": 0.001,
    "UKOIL": 0.10,
    "GER40": 10.0,
    "NAS100": 25.0,
    "SPX500": 5.0,
}


def _auction_tick_size(symbol: Instrument | str) -> float:
    raw = getattr(symbol, "value", symbol)
    return AUCTION_TICK_SIZE_BY_SYMBOL.get(str(raw), AUCTION_DEFAULT_TICK_SIZE)


# =============================================================================
# STATE MODELS
# =============================================================================

class SymbolRunStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY_PENDING = "retry_pending"
    SKIPPED = "skipped"


@dataclass(slots=True)
class SymbolState:
    symbol: str
    status: str = SymbolRunStatus.PENDING.value
    started_at: str | None = None
    completed_at: str | None = None
    error_message: str | None = None
    retry_after_utc: str | None = None
    refreshed_timeframes: list[str] = field(default_factory=list)
    analysis_snapshot: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BatchState:
    batch_index: int = 0
    batch_size: int = 1
    total_batches: int = 1
    auto_mode: bool = True
    force_batch: int | None = None
    current_batch_symbols: list[str] = field(default_factory=list)
    current_batch_started_at: str | None = None
    current_batch_completed_at: str | None = None
    symbol_states: dict[str, SymbolState] = field(default_factory=dict)
    last_run_status: str = "idle"
    last_error: str | None = None
    updated_at: str | None = None

    def is_current_batch_complete(self) -> bool:
        if not self.current_batch_symbols:
            return False

        return all(
            symbol in self.symbol_states
            and self.symbol_states[symbol].status in {
                SymbolRunStatus.SUCCESS.value,
                SymbolRunStatus.SKIPPED.value,
            }
            for symbol in self.current_batch_symbols
        )


# =============================================================================
# NORMALIZED CLOUD CONTRACTS
# =============================================================================

@dataclass(slots=True)
class InstrumentCycleResult:
    symbol: str
    status: str = "ok"
    price: float | None = None
    market_state: str | None = None
    htf_bias: str | None = None
    phase: str | None = None

    setup: str | None = None
    setup_status: str | None = None
    direction: str | None = None
    confidence: float | None = None

    scenario_type: str | None = None
    scenario_probability: float | None = None

    final_signal: str = "IDLE"
    watch_status: str = "-"
    watch_reason: str | None = None
    behavioral_summary: str | None = None
    invalidation_level: float | None = None
    target_zone: list[float] = field(default_factory=list)

    execution_status: str | None = None
    execution_model: str | None = None
    risk_reward_ratio: float | None = None
    entry_reference_price: float | None = None
    invalidation_reference_price: float | None = None
    target_reference_price: float | None = None

    refreshed_timeframes: list[str] = field(default_factory=list)
    consistency_ok: bool | None = None
    consistency_score: float | None = None
    conflict_flags: list[str] = field(default_factory=list)
    consistency_warnings: list[str] = field(default_factory=list)
    consistency_summary: str | None = None

    data_status: str | None = None
    analysis_snapshot: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None
    alert_payload: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(asdict(self))


@dataclass(slots=True)
class CycleResult:
    cycle_id: str
    started_at: str
    finished_at: str
    status: str
    instruments: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(asdict(self))


# =============================================================================
# RATE LIMIT
# =============================================================================

@dataclass(slots=True)
class MinuteBudget:
    limit_per_minute: int
    used_in_window: int = 0
    window_started_at: datetime | None = None

    def can_spend(self, credits: int = 1) -> bool:
        self._rollover_if_needed()
        return (self.used_in_window + credits) <= self.limit_per_minute

    def spend(self, credits: int = 1) -> None:
        self._rollover_if_needed()
        self.used_in_window += credits

    def seconds_until_reset(self) -> float:
        self._rollover_if_needed()
        if self.window_started_at is None:
            return 0.0
        reset_at = self.window_started_at + timedelta(minutes=1)
        return max(0.0, (reset_at - datetime.now(UTC)).total_seconds())

    def _rollover_if_needed(self) -> None:
        now = datetime.now(UTC)
        if self.window_started_at is None:
            self.window_started_at = now
            self.used_in_window = 0
            return

        if (now - self.window_started_at).total_seconds() >= 60:
            self.window_started_at = now
            self.used_in_window = 0


class TwelveDataRateLimitError(RuntimeError):
    """Raised when provider minute credits are exhausted."""


# =============================================================================
# SERIALIZATION HELPERS
# =============================================================================

def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def is_weekend_utc(dt: datetime | None = None) -> bool:
    ref = dt or datetime.now(UTC)
    return ref.weekday() >= 5


def should_skip_for_weekend(symbol: Instrument, dt: datetime | None = None) -> bool:
    """
    Weekend market policy:
    - Crypto (BTCUSD, ETHUSD) працює 24/7 -> НЕ скіпаємо
    - Всі інші інструменти на вихідних -> скіпаємо
    """
    if not is_weekend_utc(dt):
        return False

    crypto_symbols = {
        Instrument.BTCUSD,
        Instrument.ETHUSD,
    }

    return symbol not in crypto_symbols


def to_jsonable(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, list):
        return [to_jsonable(item) for item in value]

    if isinstance(value, tuple):
        return [to_jsonable(item) for item in value]

    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}

    if is_dataclass(value):
        return {key: to_jsonable(item) for key, item in asdict(value).items()}

    if hasattr(value, "model_dump"):
        return to_jsonable(value.model_dump())

    if isinstance(value, pd.DataFrame):
        if value.empty:
            return {"rows": 0, "last_ts": None, "last_close": None}

        last_ts = value.index[-1]
        last_close = None
        if "close" in value.columns:
            try:
                last_close = float(value.iloc[-1]["close"])
            except Exception:
                last_close = None

        return {
            "rows": len(value),
            "last_ts": str(last_ts),
            "last_close": last_close,
        }

    if isinstance(value, pd.Series):
        return value.to_dict()

    return str(value)


def build_market_closed_journal_record(
    symbol: Instrument,
    reason: str,
    refreshed_timeframes: list[str],
    *,
    simulation_mode: bool,
) -> dict[str, Any]:
    return {
        "ts": now_iso(),
        "instrument": symbol.value,
        "price": None,
        "market_state": None,
        "htf_bias": None,
        "phase": None,
        "context": None,
        "setups": None,
        "scenario": {
            "type": "MARKET_CLOSED",
            "phase": None,
            "decision": "SKIPPED",
            "next_expected_event": None,
            "missing_conditions": [],
            "alignment_score": 0.0,
            "evidence": None,
            "execution": None,
        },
        "final_signal": {
            "setup": None,
            "status": "SKIPPED",
            "direction": None,
            "confidence": 0.0,
        },
        "behavioral_summary": {
            "dominant_scenario": "MARKET_CLOSED",
            "decision": "SKIPPED",
            "missing_conditions": [],
            "next_expected_event": None,
            "alignment_score": 0.0,
        },
        "meta": {
            "simulation_mode": simulation_mode,
            "refreshed_timeframes": refreshed_timeframes,
            "data_source": "market_closed",
            "reason": reason,
        },
    }


def extract_last_bar_high(df: pd.DataFrame | None):
    if df is None or df.empty:
        return None
    if "high" not in df.columns:
        return None
    try:
        return float(df.iloc[-1]["high"])
    except Exception:
        return None


def extract_last_bar_low(df: pd.DataFrame | None):
    if df is None or df.empty:
        return None
    if "low" not in df.columns:
        return None
    try:
        return float(df.iloc[-1]["low"])
    except Exception:
        return None


def _safe_float_for_alert(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_telegram_trade_alert_allowed(payload: dict[str, Any]) -> tuple[bool, str]:
    """
    Hard Telegram gate.

    Telegram is allowed only for mature trade candidates:
    READY + EXECUTABLE + valid direction + complete geometry + RR 2..10.

    Everything else still goes to journal/statistics/open_signals,
    but stays silent in Telegram.
    """
    if not isinstance(payload, dict):
        return False, "payload_is_not_dict"

    signal_class = str(payload.get("signal_class") or payload.get("stage") or "").upper()
    execution_status = str(payload.get("execution_status") or "").upper()
    direction = str(payload.get("direction") or "").upper()
    scenario = str(payload.get("scenario") or payload.get("scenario_type") or "").upper()

    rr = _safe_float_for_alert(
        payload.get("risk_reward_ratio")
        or payload.get("practical_rr")
        or payload.get("rr")
        or payload.get("rr_ratio")
        or payload.get("risk_reward")
        or payload.get("expected_rr")
        or payload.get("planned_rr")
    )
    if rr is not None:
        payload["risk_reward_ratio"] = rr
        payload.setdefault("practical_rr", rr)
    confidence = _safe_float_for_alert(payload.get("confidence") or payload.get("probability"))

    entry = _safe_float_for_alert(
        payload.get("entry_reference_price")
        or payload.get("entry")
    )
    stop = _safe_float_for_alert(
        payload.get("invalidation_reference_price")
        or payload.get("stop_loss")
        or payload.get("stop")
    )
    target = _safe_float_for_alert(
        payload.get("target_reference_price")
        or payload.get("take_profit")
        or payload.get("target")
    )
    current = _safe_float_for_alert(
        payload.get("current_price")
        or payload.get("last_price")
        or payload.get("price")
        or payload.get("close")
    )

    if scenario in {"", "NO_ACTION", "MARKET_CLOSED"}:
        return False, f"blocked_scenario:{scenario or '-'}"

    if signal_class != "READY":
        return False, f"blocked_non_ready_signal_class:{signal_class or '-'}"

    if execution_status != "EXECUTABLE":
        return False, f"blocked_non_executable_status:{execution_status or '-'}"

    if direction not in {"LONG", "SHORT"}:
        return False, f"blocked_invalid_direction:{direction or '-'}"

    if confidence is None:
        return False, "blocked_missing_confidence"

    if confidence > 1.0:
        confidence = confidence / 100.0

    if confidence < TELEGRAM_MIN_CONFIDENCE:
        return False, f"blocked_confidence_too_low:{confidence:.2f}"

    if rr is None:
        return False, "blocked_missing_rr"

    if rr < TELEGRAM_MIN_RR:
        return False, f"blocked_rr_too_low:{rr:.2f}"

    if rr > TELEGRAM_MAX_RR:
        return False, f"blocked_rr_too_high:{rr:.2f}"

    if entry is None or stop is None or target is None:
        return False, "blocked_missing_trade_geometry"

    if direction == "LONG" and not (stop < entry < target):
        return False, "blocked_invalid_long_geometry"

    if direction == "SHORT" and not (target < entry < stop):
        return False, "blocked_invalid_short_geometry"

    if current is not None:
        risk = abs(stop - entry)
        if risk > 0:
            entry_distance = current - entry
            entry_distance_r = entry_distance / risk
            already_moved_r = 0.0

            if direction == "LONG" and current > entry:
                already_moved_r = (current - entry) / risk
            elif direction == "SHORT" and current < entry:
                already_moved_r = (entry - current) / risk

            payload["current_price"] = current
            payload["entry_distance"] = round(entry_distance, 8)
            payload["entry_distance_R"] = round(entry_distance_r, 6)
            payload["already_moved_R"] = round(already_moved_r, 6)
            payload["execution_timing_guard_version"] = EXECUTION_TIMING_GUARD_VERSION

            if already_moved_r >= TELEGRAM_HARD_LATE_SIGNAL_THRESHOLD_R:
                payload["entry_timing_status"] = "HARD_LATE_SIGNAL"
                payload["wait_retest_only"] = True
                payload["entry_retest_required"] = True
                payload["late_signal_reason"] = f"price_already_moved_{already_moved_r:.2f}R_from_entry"
                return False, "blocked_late_signal_wait_retest_only"

            if already_moved_r >= TELEGRAM_LATE_SIGNAL_THRESHOLD_R:
                payload["entry_timing_status"] = "LATE_SIGNAL"
                payload["wait_retest_only"] = True
                payload["entry_retest_required"] = True
                payload["late_signal_reason"] = f"price_already_moved_{already_moved_r:.2f}R_from_entry"
                return False, "blocked_late_signal_wait_retest_only"

            payload["entry_timing_status"] = "ENTRY_ACTIONABLE"
            payload["wait_retest_only"] = False
            payload["entry_retest_required"] = False
            payload["late_signal_reason"] = None

    return True, "telegram_allowed_ready_executable_trade"


def _map_instrument_to_formatter_payload(inst: dict[str, Any]) -> Optional[dict[str, Any]]:
    alert_payload = inst.get("alert_payload")
    if not alert_payload:
        return None

    if not alert_payload.get("should_alert", False):
        return None

    allowed, _reason = _is_telegram_trade_alert_allowed(alert_payload)
    if not allowed:
        return None

    if alert_payload.get("telegram_allowed") is not True:
        return None

    return alert_payload


# =============================================================================
# STATE I/O
# =============================================================================

def load_state(path: Path | None = None) -> BatchState:
    path = path or settings.runner_state_path
    if not path.exists():
        return BatchState(updated_at=now_iso())

    raw = json.loads(path.read_text(encoding="utf-8"))
    symbol_states = {
        symbol: SymbolState(**payload)
        for symbol, payload in raw.get("symbol_states", {}).items()
    }

    return BatchState(
        batch_index=raw.get("batch_index", 0),
        batch_size=raw.get("batch_size", 1),
        total_batches=raw.get("total_batches", 1),
        auto_mode=raw.get("auto_mode", True),
        force_batch=raw.get("force_batch"),
        current_batch_symbols=raw.get("current_batch_symbols", []),
        current_batch_started_at=raw.get("current_batch_started_at"),
        current_batch_completed_at=raw.get("current_batch_completed_at"),
        symbol_states=symbol_states,
        last_run_status=raw.get("last_run_status", "idle"),
        last_error=raw.get("last_error"),
        updated_at=raw.get("updated_at"),
    )


def save_state(state: BatchState, path: Path | None = None) -> None:
    path = path or settings.runner_state_path
    state.updated_at = now_iso()
    payload = asdict(state)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp_path.replace(path)


# =============================================================================
# RUNNER
# =============================================================================

class StatefulBatchRunner:
    """
    Orchestration-only runner with guaranteed history writes.
    """

    def __init__(
        self,
        loader: MarketDataLoader,
        *,
        state_path: Path | None = None,
        minute_limit: int = 8,
        batch_size: int = 1,
        auto_mode: bool = True,
        force_batch: int | None = None,
        simulation_mode: bool = False,
        instrument_profiles: list[dict[str, Any]] | None = None,
        timeframes_by_symbol: dict[Instrument, list[Timeframe]] | None = None,
        batch_group: str = "core",
    ) -> None:
        self.loader = loader
        self.state_path = state_path or settings.runner_state_path
        self.simulation_mode = simulation_mode
        self.batch_group = batch_group

        self.state = load_state(self.state_path)
        self.state.batch_size = batch_size
        self.state.auto_mode = auto_mode
        self.state.force_batch = force_batch

        self.budget = MinuteBudget(limit_per_minute=minute_limit)
        self.signal_tracker = SignalTracker(
            open_signals_path=str(RUNTIME_DIR / "open_signals.json")
        )
        self.telegram = build_telegram_notifier()

        self.timeframes_by_symbol = timeframes_by_symbol or DEFAULT_TIMEFRAMES_BY_SYMBOL

        if instrument_profiles is not None:
            selected_profiles = sorted(
                instrument_profiles,
                key=lambda item: (item.get("priority", 999), self._instrument_sort_key(item["symbol"])),
            )
        else:
            batch_symbols_raw = get_batch_symbols(batch_group)
            batch_symbols = [self._normalize_batch_symbol(sym) for sym in batch_symbols_raw]
            selected_profiles = [
                {"symbol": symbol, "priority": 1}
                for symbol in batch_symbols
            ]

        self.instrument_profiles = selected_profiles
        self.batches = self._make_batches(self.instrument_profiles, batch_size)
        self.state.total_batches = len(self.batches)

    # -------------------------------------------------------------------------
    # guaranteed history helpers
    # -------------------------------------------------------------------------

    def _safe_write_cycle_history(
        self,
        *,
        event: str,
        cycle_id: str,
        batch_id: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        payload = {
            "event_id": f"{event}_{cycle_id}_{batch_id}_{int(time.time() * 1000)}",
            "event_type": event,
            "ts_utc": now_iso(),
            "cycle_id": cycle_id,
            "batch_id": batch_id,
            "runner_version": RUNNER_VERSION,
            "symbol": "-",
            "timeframe": "15m",
            "source": "stateful_batch_runner_fallback",
            "status": "ok",
            "payload": to_jsonable(extra or {}),
        }
        try:
            safe_append_ndjson(JOURNAL_FALLBACK_PATH, payload)
        except Exception as e:
            print(f"[FALLBACK JOURNAL WRITE ERROR] {e}")

    def _safe_write_symbol_history(
        self,
        *,
        symbol: str,
        cycle_id: str,
        batch_id: str,
        snapshot_payload: dict[str, Any] | None = None,
        journal_event: str = "symbol_checkpoint",
        journal_payload: dict[str, Any] | None = None,
    ) -> None:
        if snapshot_payload is not None:
            snapshot_record = to_jsonable(snapshot_payload)
            try:
                safe_append_ndjson(SNAPSHOT_FALLBACK_PATH, snapshot_record)
            except Exception as e:
                print(f"[FALLBACK SNAPSHOT WRITE ERROR] {symbol}: {e}")

        event_record = {
            "event_id": f"{journal_event}_{symbol}_{cycle_id}_{int(time.time() * 1000)}",
            "event_type": journal_event,
            "ts_utc": now_iso(),
            "cycle_id": cycle_id,
            "batch_id": batch_id,
            "runner_version": RUNNER_VERSION,
            "symbol": symbol,
            "timeframe": "15m",
            "source": "stateful_batch_runner_fallback",
            "status": "ok",
            "payload": to_jsonable(journal_payload or {}),
        }
        try:
            safe_append_ndjson(JOURNAL_FALLBACK_PATH, event_record)
        except Exception as e:
            print(f"[FALLBACK JOURNAL WRITE ERROR] {symbol}: {e}")

    def _build_fallback_snapshot_record(
        self,
        *,
        symbol: str,
        cycle_id: str,
        batch_id: str,
        payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        raw = to_jsonable(payload or {})
        if not isinstance(raw, dict):
            raw = {"raw_payload": raw}

        record = {
            "schema_version": raw.get("schema_version", "2.0"),
            "ts": raw.get("ts", now_iso()),
            "cycle_id": raw.get("cycle_id", cycle_id),
            "batch_id": raw.get("batch_id", batch_id),
            "runner_version": raw.get("runner_version", RUNNER_VERSION),
            "instrument": raw.get("instrument", symbol),
            "timeframe": raw.get("timeframe", "15m"),
            "price": raw.get("price"),
            "market_state": raw.get("market_state"),
            "htf_bias": raw.get("htf_bias"),
            "phase": raw.get("phase"),
            "context": raw.get("context", {}),
            "setups": raw.get("setups", {}),
            "scenario": raw.get("scenario", {}),
            "final_signal": raw.get("final_signal", {}),
            "behavioral_summary": raw.get("behavioral_summary", {}),
            "consistency": raw.get("consistency", {}),
            "auction_context": raw.get("auction_context") or (raw.get("context", {}) or {}).get("auction", {}).get("context"),
            "auction_filters": raw.get("auction_filters") or (raw.get("context", {}) or {}).get("auction", {}).get("filters"),
            "auction_telemetry_mode": raw.get("auction_telemetry_mode") or (raw.get("context", {}) or {}).get("auction", {}).get("telemetry_mode"),
            "meta": raw.get("meta", {}),
        }
        return record

    def run(self) -> None:
        result = self.run_batch_cycle()
        print("\n========================================================================================")
        print("STATEFUL BATCH MULTI-INSTRUMENT RUN COMPLETE")
        print("========================================================================================")
        print(json.dumps(
            {
                "cycle_id": result.get("cycle_id"),
                "status": result.get("status"),
                "batch_group": result.get("meta", {}).get("batch_group"),
                "instrument_count": len(result.get("instruments", [])),
                "error_count": len(result.get("errors", [])),
                "skipped_count": result.get("meta", {}).get("skipped_count", 0),
            },
            ensure_ascii=False,
            indent=2,
        ))

    def run_batch_cycle(self) -> dict[str, Any]:
        started_at = now_iso()
        cycle_id = started_at
        cycle_logger = bind_logger(logger, cycle_id=cycle_id, symbol="-")
        cycle_started_monotonic = time.monotonic()

        if not self.batches:
            cycle_logger.warning("No instruments configured.")
            return CycleResult(
                cycle_id=cycle_id,
                started_at=started_at,
                finished_at=now_iso(),
                status="skipped",
                instruments=[],
                errors=[],
                meta={
                    "reason": "no_instruments_configured",
                    "batch_group": self.batch_group,
                    "skipped_count": 0,
                },
            ).to_dict()

        batch_index = (
            self.state.force_batch
            if self.state.force_batch is not None
            else self.state.batch_index
        )
        if batch_index >= len(self.batches):
            batch_index = 0

        current_batch = self.batches[batch_index]
        current_symbols = [item["symbol"].value for item in current_batch]
        batch_id = f"{self.batch_group}_batch_{batch_index}_of_{self.state.total_batches}"

        self._ensure_batch_initialized(batch_index=batch_index, current_symbols=current_symbols)
        self._print_header(batch_index, current_batch)

        write_cycle_started(
            cycle_id=cycle_id,
            batch_id=batch_id,
            runner_version=RUNNER_VERSION,
            instruments=current_symbols,
            batch_size=self.state.batch_size,
            auto_mode=self.state.auto_mode,
            simulation_mode=self.simulation_mode,
        )
        self._safe_write_cycle_history(
            event="cycle_started_fallback",
            cycle_id=cycle_id,
            batch_id=batch_id,
            extra={
                "batch_group": self.batch_group,
                "instruments": current_symbols,
                "batch_size": self.state.batch_size,
                "auto_mode": self.state.auto_mode,
                "simulation_mode": self.simulation_mode,
            },
        )

        normalized_instruments: list[dict[str, Any]] = []
        cycle_errors: list[dict[str, Any]] = []
        batch_had_errors = False
        processed_count = 0
        error_count = 0
        skipped_count = 0
        alerts_count = 0

        try:
            for item in current_batch:
                symbol: Instrument = item["symbol"]
                symbol_state = self.state.symbol_states.setdefault(
                    symbol.value,
                    SymbolState(symbol=symbol.value),
                )

                if symbol_state.status in {
                    SymbolRunStatus.SUCCESS.value,
                    SymbolRunStatus.SKIPPED.value,
                }:
                    print(f"\n### SKIPPING {symbol.value} (already completed in current batch)")
                    skipped_result = InstrumentCycleResult(
                        symbol=symbol.value,
                        status="skipped",
                        final_signal="IDLE",
                        watch_status="-",
                        analysis_snapshot=to_jsonable(symbol_state.analysis_snapshot),
                    )
                    normalized_instruments.append(skipped_result.to_dict())
                    processed_count += 1
                    skipped_count += 1
                    continue

                try:
                    instrument_result = self._analyze_symbol(
                        symbol,
                        cycle_id=cycle_id,
                        batch_id=batch_id,
                    )
                    normalized_instruments.append(instrument_result.to_dict())
                    processed_count += 1

                    if instrument_result.alert_payload:
                        sent = self._dispatch_alert_payload(instrument_result.alert_payload)
                        if sent:
                            alerts_count += 1

                    if instrument_result.status == "skipped":
                        skipped_count += 1
                    elif instrument_result.status != "ok":
                        batch_had_errors = True
                        error_count += 1
                        cycle_errors.append(
                            {
                                "symbol": symbol.value,
                                "type": "symbol_analysis_error",
                                "error_message": instrument_result.error_message,
                            }
                        )

                except TwelveDataRateLimitError as error:
                    batch_had_errors = True
                    error_count += 1
                    retry_after = datetime.now(UTC) + timedelta(
                        seconds=self.budget.seconds_until_reset() + 1
                    )

                    symbol_state.status = SymbolRunStatus.RETRY_PENDING.value
                    symbol_state.error_message = str(error)
                    symbol_state.retry_after_utc = retry_after.isoformat()
                    symbol_state.completed_at = None

                    self.state.last_run_status = "partial_error"
                    self.state.last_error = str(error)
                    save_state(self.state, self.state_path)

                    print(f"  [ERROR] {symbol.value}: {error}")

                    normalized_instruments.append(
                        InstrumentCycleResult(
                            symbol=symbol.value,
                            status="error",
                            final_signal="IDLE",
                            watch_status="-",
                            error_message=str(error),
                            data_status="rate_limit_error",
                        ).to_dict()
                    )

                    cycle_errors.append(
                        {
                            "symbol": symbol.value,
                            "type": "rate_limit_error",
                            "error_message": str(error),
                            "retry_after_utc": retry_after.isoformat(),
                        }
                    )
                    self._safe_write_symbol_history(
                        symbol=symbol.value,
                        cycle_id=cycle_id,
                        batch_id=batch_id,
                        snapshot_payload=self._build_fallback_snapshot_record(
                            symbol=symbol.value,
                            cycle_id=cycle_id,
                            batch_id=batch_id,
                            payload={
                                "instrument": symbol.value,
                                "ts": now_iso(),
                                "cycle_id": cycle_id,
                                "batch_id": batch_id,
                                "runner_version": RUNNER_VERSION,
                                "timeframe": "15m",
                                "price": None,
                                "market_state": None,
                                "htf_bias": None,
                                "phase": None,
                                "context": {},
                                "setups": {},
                                "scenario": {"type": "RATE_LIMIT_ERROR"},
                                "final_signal": {"status": "ERROR"},
                                "behavioral_summary": {"decision": "WAIT"},
                                "consistency": {},
                                "meta": {"error": str(error), "batch_group": self.batch_group},
                            },
                        ),
                        journal_event="symbol_rate_limit_error_fallback",
                        journal_payload={
                            "error": str(error),
                            "retry_after_utc": retry_after.isoformat(),
                            "batch_group": self.batch_group,
                        },
                    )
                    break

                except Exception as error:
                    batch_had_errors = True
                    error_count += 1

                    symbol_state.status = SymbolRunStatus.FAILED.value
                    symbol_state.error_message = str(error)
                    symbol_state.completed_at = None

                    self.state.last_run_status = "partial_error"
                    self.state.last_error = str(error)
                    save_state(self.state, self.state_path)

                    print(f"  [ERROR] {symbol.value}: {error}")

                    normalized_instruments.append(
                        InstrumentCycleResult(
                            symbol=symbol.value,
                            status="error",
                            final_signal="IDLE",
                            watch_status="-",
                            error_message=str(error),
                            data_status="exception",
                        ).to_dict()
                    )

                    cycle_errors.append(
                        {
                            "symbol": symbol.value,
                            "type": "symbol_exception",
                            "error_message": str(error),
                        }
                    )
                    self._safe_write_symbol_history(
                        symbol=symbol.value,
                        cycle_id=cycle_id,
                        batch_id=batch_id,
                        snapshot_payload=self._build_fallback_snapshot_record(
                            symbol=symbol.value,
                            cycle_id=cycle_id,
                            batch_id=batch_id,
                            payload={
                                "instrument": symbol.value,
                                "ts": now_iso(),
                                "cycle_id": cycle_id,
                                "batch_id": batch_id,
                                "runner_version": RUNNER_VERSION,
                                "timeframe": "15m",
                                "price": None,
                                "market_state": None,
                                "htf_bias": None,
                                "phase": None,
                                "context": {},
                                "setups": {},
                                "scenario": {"type": "SYMBOL_EXCEPTION"},
                                "final_signal": {"status": "ERROR"},
                                "behavioral_summary": {"decision": "WAIT"},
                                "consistency": {},
                                "meta": {"error": str(error), "batch_group": self.batch_group},
                            },
                        ),
                        journal_event="symbol_exception_fallback",
                        journal_payload={"error": str(error), "batch_group": self.batch_group},
                    )
                    continue

            if not batch_had_errors and self.state.is_current_batch_complete():
                self.state.current_batch_completed_at = now_iso()
                self.state.last_run_status = "success"
                self.state.last_error = None
                self._advance_batch_pointer()
                save_state(self.state, self.state_path)

                if skipped_count > 0 and error_count == 0:
                    print("\nBatch completed with skipped instruments only/partially. State advanced to next batch.")
                else:
                    print("\nBatch completed successfully. State advanced to next batch.")

                cycle_status = "ok"
            else:
                self.state.last_run_status = "partial_error"
                save_state(self.state, self.state_path)
                print("\nБули помилки — стан batch не оновлюється, щоб не пропустити поточний batch.")
                cycle_status = "partial" if normalized_instruments else "error"

            elapsed_sec = time.monotonic() - cycle_started_monotonic

            write_cycle_finished(
                cycle_id=cycle_id,
                batch_id=batch_id,
                runner_version=RUNNER_VERSION,
                processed=processed_count,
                errors=error_count,
                alerts=alerts_count,
                duration_sec=elapsed_sec,
            )
            self._safe_write_cycle_history(
                event="cycle_finished_fallback",
                cycle_id=cycle_id,
                batch_id=batch_id,
                extra={
                    "batch_group": self.batch_group,
                    "processed": processed_count,
                    "errors": error_count,
                    "skipped": skipped_count,
                    "alerts": alerts_count,
                    "duration_sec": round(elapsed_sec, 3),
                    "status": cycle_status,
                },
            )

            cycle_logger.info(
                "Heavy statistics export is disabled in live runner. "
                "Use app.services.lightweight_statistics_exporter or scheduled reporting jobs."
            )

            cycle_logger.info(
                f"Batch cycle finished. batch_group={self.batch_group} status={cycle_status} instruments={len(normalized_instruments)} errors={len(cycle_errors)} skipped={skipped_count}"
            )

            return CycleResult(
                cycle_id=cycle_id,
                started_at=started_at,
                finished_at=now_iso(),
                status=cycle_status,
                instruments=normalized_instruments,
                errors=cycle_errors,
                meta={
                    "batch_group": self.batch_group,
                    "batch_index": batch_index,
                    "batch_size": self.state.batch_size,
                    "total_batches": self.state.total_batches,
                    "auto_mode": self.state.auto_mode,
                    "force_batch": self.state.force_batch,
                    "simulation_mode": self.simulation_mode,
                    "current_batch_symbols": current_symbols,
                    "skipped_count": skipped_count,
                },
            ).to_dict()

        except Exception as cycle_error:
            self._safe_write_cycle_history(
                event="cycle_crashed_fallback",
                cycle_id=cycle_id,
                batch_id=batch_id,
                extra={"error": str(cycle_error), "batch_group": self.batch_group},
            )
            raise

    def _analyze_symbol(
        self,
        symbol: Instrument,
        *,
        cycle_id: str,
        batch_id: str,
    ) -> InstrumentCycleResult:
        symbol_logger = bind_logger(logger, cycle_id=cycle_id, symbol=symbol.value)

        print(f"\n### ANALYZING {symbol.value}")
        print("----------------------------------------------------------------------------------------")

        symbol_state = self.state.symbol_states[symbol.value]
        symbol_state.started_at = symbol_state.started_at or now_iso()
        symbol_state.error_message = None
        symbol_state.retry_after_utc = None
        save_state(self.state, self.state_path)

        fallback_snapshot_payload: dict[str, Any] | None = None
        fallback_journal_payload: dict[str, Any] = {"status": "started", "batch_group": self.batch_group}

        self._safe_write_symbol_history(
            symbol=symbol.value,
            cycle_id=cycle_id,
            batch_id=batch_id,
            snapshot_payload=None,
            journal_event="symbol_started_fallback",
            journal_payload={"status": "started", "batch_group": self.batch_group},
        )

        try:
            timeframes = self.timeframes_by_symbol.get(
                symbol,
                [Timeframe.M15, Timeframe.M30, Timeframe.H1, Timeframe.H4, Timeframe.D1],
            )

            refreshed_timeframes: list[str] = []
            load_results: dict[str, Any] = {}

            # -------------------------------------------------------------
            # SMART WEEKEND SKIP: skip early for non-crypto markets
            # -------------------------------------------------------------
            if should_skip_for_weekend(symbol):
                print(f"  [SKIP] {symbol.value}: weekend market closed")

                journal_record = build_market_closed_journal_record(
                    symbol=symbol,
                    reason="WEEKEND_MARKET_CLOSED",
                    refreshed_timeframes=refreshed_timeframes,
                    simulation_mode=self.simulation_mode,
                )
                journal_record["cycle_id"] = cycle_id
                journal_record["batch_id"] = batch_id
                journal_record["runner_version"] = RUNNER_VERSION
                journal_record["batch_group"] = self.batch_group
                journal_record["timeframe"] = "15m"
                journal_record["schema_version"] = "2.0"

                symbol_state.refreshed_timeframes = refreshed_timeframes
                symbol_state.analysis_snapshot = {
                    "batch_group": self.batch_group,
                    "load_results": to_jsonable(load_results),
                    "analysis": None,
                    "behavioral_journal": to_jsonable(journal_record),
                    "refreshed_timeframes": refreshed_timeframes,
                    "completed_at": now_iso(),
                    "skipped": True,
                    "skip_reason": "WEEKEND_MARKET_CLOSED",
                }
                symbol_state.status = SymbolRunStatus.SKIPPED.value
                symbol_state.completed_at = now_iso()

                write_instrument_analyzed(
                    cycle_id=cycle_id,
                    batch_id=batch_id,
                    runner_version=RUNNER_VERSION,
                    symbol=symbol.value,
                    timeframe="15m",
                    analysis_payload={
                        "batch_group": self.batch_group,
                        "symbol": symbol.value,
                        "price": None,
                        "market_state": None,
                        "htf_bias": None,
                        "phase": None,
                        "scenario_type": "MARKET_CLOSED",
                        "scenario_decision": "SKIPPED",
                        "final_signal_status": "SKIPPED",
                        "final_signal_direction": None,
                        "final_signal_confidence": 0.0,
                        "consistency_ok": True,
                        "consistency_score": 1.0,
                    },
                )

                write_instrument_snapshot(
                    cycle_id=cycle_id,
                    batch_id=batch_id,
                    runner_version=RUNNER_VERSION,
                    symbol=symbol.value,
                    timeframe="15m",
                    analysis_payload=journal_record,
                )

                fallback_snapshot_payload = journal_record
                fallback_journal_payload = {
                    "status": "skipped",
                    "reason": "WEEKEND_MARKET_CLOSED",
                    "batch_group": self.batch_group,
                }

                save_state(self.state, self.state_path)

                return InstrumentCycleResult(
                    symbol=symbol.value,
                    status="skipped",
                    price=None,
                    market_state=None,
                    htf_bias=None,
                    phase=None,
                    setup=None,
                    setup_status="SKIPPED",
                    direction=None,
                    confidence=0.0,
                    scenario_type="MARKET_CLOSED",
                    scenario_probability=0.0,
                    final_signal="IDLE",
                    watch_status="-",
                    watch_reason="WEEKEND_MARKET_CLOSED",
                    behavioral_summary="MARKET_CLOSED",
                    refreshed_timeframes=refreshed_timeframes,
                    consistency_ok=True,
                    consistency_score=1.0,
                    conflict_flags=[],
                    consistency_warnings=[],
                    consistency_summary="Market closed",
                    data_status="market_closed",
                    analysis_snapshot=to_jsonable(symbol_state.analysis_snapshot),
                    error_message=None,
                    alert_payload=None,
                )

            series_by_tf: dict[Timeframe, pd.DataFrame] = {}

            for timeframe in timeframes:
                if not self.simulation_mode:
                    self._ensure_budget_or_wait(credits=1)

                result = self._load_timeframe(symbol, timeframe)

                if getattr(result, "source", None) == "api":
                    self.budget.spend(1)
                    refreshed_timeframes.append(timeframe.value)

                df = getattr(result, "df", None)
                if df is None:
                    raise RuntimeError(f"Loader returned no dataframe for {symbol.value} {timeframe.value}")

                series_by_tf[timeframe] = df
                load_results[timeframe.value] = {
                    "source": getattr(result, "source", None),
                    "rows": getattr(result, "rows", len(df)),
                    "last_ts": getattr(result, "last_ts", None),
                    "last_close": getattr(result, "last_close", None),
                }

            analysis = self._run_analysis_pipeline(symbol, series_by_tf)

            consistency_payload = self._build_consistency_payload(
                context=analysis["context"],
                setups=analysis["setups"],
                final_signal=analysis["final_signal"],
            )

            consistency = check_consistency(
                symbol=symbol.value,
                market_state=consistency_payload["market_state"],
                htf_bias=consistency_payload["htf_bias"],
                phase=consistency_payload["phase"],
                final_signal_setup=consistency_payload["final_signal_setup"],
                final_signal_status=consistency_payload["final_signal_status"],
                final_signal_direction=consistency_payload["final_signal_direction"],
                diagnostics=consistency_payload["diagnostics"],
                behavioral_summary=consistency_payload["behavioral_summary"],
            )

            journal_record = self._build_behavioral_journal_record(
                symbol=symbol,
                context=analysis["context"],
                setups=analysis["setups"],
                scenario=analysis["scenario"],
                final_signal=analysis["final_signal"],
                refreshed_timeframes=refreshed_timeframes,
                auction_context=analysis.get("auction_context"),
                auction_filters=analysis.get("auction_filters"),
                auction_telemetry_mode=analysis.get("auction_telemetry_mode"),
            )

            journal_record["consistency"] = to_jsonable(consistency.to_dict())
            journal_record["cycle_id"] = cycle_id
            journal_record["batch_id"] = batch_id
            journal_record["runner_version"] = RUNNER_VERSION
            journal_record["batch_group"] = self.batch_group
            journal_record["timeframe"] = "15m"
            journal_record["schema_version"] = "2.0"

            symbol_state.refreshed_timeframes = refreshed_timeframes
            symbol_state.analysis_snapshot = {
                "batch_group": self.batch_group,
                "load_results": to_jsonable(load_results),
                "analysis": to_jsonable(analysis),
                "behavioral_journal": to_jsonable(journal_record),
                "refreshed_timeframes": refreshed_timeframes,
                "completed_at": now_iso(),
            }
            symbol_state.status = SymbolRunStatus.SUCCESS.value
            symbol_state.completed_at = now_iso()

            context = analysis.get("context")
            scenario = analysis.get("scenario")
            final_signal = analysis.get("final_signal")
            setups = analysis.get("setups") or []

            price = extract_context_price(context)
            market_state = extract_context_market_state(context)
            htf_bias = extract_context_htf_bias(context)
            phase = classify_market_phase(context)

            scenario_ok = scenario is not None and not (
                isinstance(scenario, dict) and scenario.get("scenario_engine_failed")
            )

            if scenario_ok:
                scenario_type = self._safe_attr(scenario, "scenario_type")
                scenario_type = getattr(scenario_type, "value", scenario_type)
                scenario_probability = self._safe_attr(scenario, "alignment_score")
                scenario_decision = self._safe_attr(scenario, "decision")
                scenario_decision = getattr(scenario_decision, "value", scenario_decision)
            else:
                scenario_type = infer_behavioral_scenario(context, setups)
                scenario_probability = infer_alignment_score(context, setups)
                scenario_decision = self._extract_scenario_decision(context, setups, scenario, final_signal)

            analysis_payload = {
                "batch_group": self.batch_group,
                "symbol": symbol.value,
                "price": price,
                "market_state": market_state,
                "htf_bias": htf_bias,
                "phase": phase,
                "scenario_type": scenario_type,
                "scenario_decision": scenario_decision,
                "final_signal_status": self._safe_attr(final_signal, "status"),
                "final_signal_direction": self._safe_attr(final_signal, "direction"),
                "final_signal_confidence": self._safe_attr(final_signal, "confidence"),
                "consistency_ok": consistency.is_consistent,
                "consistency_score": consistency.consistency_score,
                "final_signal": to_jsonable(journal_record.get("final_signal")),
                "behavioral_summary": to_jsonable(journal_record.get("behavioral_summary")),
                "consistency": to_jsonable(journal_record.get("consistency")),
                "auction_context": to_jsonable(analysis.get("auction_context")),
                "auction_filters": to_jsonable(analysis.get("auction_filters")),
                "auction_telemetry_mode": analysis.get("auction_telemetry_mode", "passive_only"),
            }

            write_instrument_analyzed(
                cycle_id=cycle_id,
                batch_id=batch_id,
                runner_version=RUNNER_VERSION,
                symbol=symbol.value,
                timeframe="15m",
                analysis_payload=analysis_payload,
            )

            write_instrument_snapshot(
                cycle_id=cycle_id,
                batch_id=batch_id,
                runner_version=RUNNER_VERSION,
                symbol=symbol.value,
                timeframe="15m",
                analysis_payload=journal_record,
            )

            fallback_snapshot_payload = journal_record
            fallback_journal_payload = {
                "status": "ok",
                "batch_group": self.batch_group,
                "market_state": market_state,
                "htf_bias": htf_bias,
                "phase": phase,
                "scenario_type": scenario_type,
                "scenario_decision": scenario_decision,
                "final_signal": to_jsonable(journal_record.get("final_signal")),
            }

            save_state(self.state, self.state_path)

            scenario_engine_failed = isinstance(scenario, dict) and bool(scenario.get("scenario_engine_failed"))
            scenario_engine_error = scenario.get("scenario_engine_error") if isinstance(scenario, dict) else None
            scenario_engine_error_type = scenario.get("scenario_engine_error_type") if isinstance(scenario, dict) else None

            tracker_source_payload = self._build_tracker_source_payload(
                source=scenario if scenario_ok else final_signal,
                symbol=symbol.value,
                batch_group=self.batch_group,
                cycle_id=cycle_id,
                price=price,
                market_state=market_state,
                htf_bias=htf_bias,
                phase=phase,
                scenario_type=scenario_type,
                scenario_probability=scenario_probability,
                scenario_decision=scenario_decision,
                scenario_engine_failed=scenario_engine_failed,
                scenario_engine_error=scenario_engine_error,
                scenario_engine_error_type=scenario_engine_error_type,
            )
            tracker_source_payload = self._attach_tpo_policy_to_payload(
                payload=tracker_source_payload,
                auction_context=analysis.get("auction_context"),
                auction_filters=analysis.get("auction_filters"),
            )
            tracker_source_payload = self._enrich_tracker_payload_with_ltf_model_bridge(
                payload=tracker_source_payload,
                series_by_tf=series_by_tf,
            )

            tracker_source_payload = self._enrich_tracker_payload_with_execution_bridge(
                payload=tracker_source_payload,
                context=context,
                scenario=scenario,
                final_signal=final_signal,
            )
            tracker_source_payload = self._enrich_tracker_payload_with_post_news_otd_bridge(
                payload=tracker_source_payload,
            )

            tracker_result = self.signal_tracker.process(
                scenario_result=tracker_source_payload,
                cycle_id=cycle_id,
            )
            tracker_result = self._force_tracker_result_symbol(
                tracker_result=tracker_result,
                symbol=symbol.value,
                batch_group=self.batch_group,
                cycle_id=cycle_id,
            )

            # SignalTracker normalizes and persists lifecycle payloads. Some
            # normalizer paths may drop non-core fields, so we re-attach the
            # current TPO / auction policy after tracking and before journal,
            # alert building, and statistics exporters consume the payload.
            if isinstance(tracker_result.payload, dict):
                tracker_result.payload = self._attach_tpo_policy_to_payload(
                    payload=tracker_result.payload,
                    auction_context=analysis.get("auction_context"),
                    auction_filters=analysis.get("auction_filters"),
                )
                tracker_result.payload = self._enrich_tracker_payload_with_ltf_model_bridge(
                    payload=tracker_result.payload,
                    series_by_tf=series_by_tf,
                )
                tracker_result.payload = self._enrich_tracker_payload_with_post_news_otd_bridge(
                    payload=tracker_result.payload,
                )

            if isinstance(tracker_result.previous_payload, dict):
                tracker_result.previous_payload = self._attach_tpo_policy_to_payload(
                    payload=tracker_result.previous_payload,
                    auction_context=analysis.get("auction_context"),
                    auction_filters=analysis.get("auction_filters"),
                )
                tracker_result.previous_payload = self._enrich_tracker_payload_with_ltf_model_bridge(
                    payload=tracker_result.previous_payload,
                    series_by_tf=series_by_tf,
                )
                tracker_result.previous_payload = self._enrich_tracker_payload_with_post_news_otd_bridge(
                    payload=tracker_result.previous_payload,
                )

            candidate_payload = tracker_result.payload
            if candidate_payload.get("signal_class") in {
                "SCENARIO_FORMING",
                "WATCH",
                "READY",
                "ACTIVE",
            }:
                write_signal_candidate_detected(
                    cycle_id=cycle_id,
                    batch_id=batch_id,
                    runner_version=RUNNER_VERSION,
                    symbol=symbol.value,
                    timeframe="15m",
                    signal_payload={
                        **candidate_payload,
                        "batch_group": self.batch_group,
                        "auction_context": to_jsonable(analysis.get("auction_context")),
                        "auction_filters": to_jsonable(analysis.get("auction_filters")),
                        "auction_telemetry_mode": analysis.get("auction_telemetry_mode", "passive_only"),
                    },
                )

            if tracker_result.action == "REGISTERED":
                write_signal_registered(
                    cycle_id=cycle_id,
                    batch_id=batch_id,
                    runner_version=RUNNER_VERSION,
                    symbol=symbol.value,
                    timeframe="15m",
                    signal_id=tracker_result.signal_id or candidate_payload.get("signal_id", ""),
                    payload={**tracker_result.payload, "batch_group": self.batch_group},
                )
            elif tracker_result.action == "UPDATED":
                write_signal_updated(
                    cycle_id=cycle_id,
                    batch_id=batch_id,
                    runner_version=RUNNER_VERSION,
                    symbol=symbol.value,
                    timeframe="15m",
                    signal_id=tracker_result.signal_id or candidate_payload.get("signal_id", ""),
                    payload={**tracker_result.payload, "batch_group": self.batch_group},
                    previous_payload=tracker_result.previous_payload,
                    changed_fields=tracker_result.changed_fields,
                )
            elif tracker_result.action == "RESOLVED":
                write_signal_resolved(
                    cycle_id=cycle_id,
                    batch_id=batch_id,
                    runner_version=RUNNER_VERSION,
                    symbol=symbol.value,
                    timeframe="15m",
                    signal_id=tracker_result.signal_id or candidate_payload.get("signal_id", ""),
                    payload={**tracker_result.payload, "batch_group": self.batch_group},
                )

            self._print_symbol_summary(symbol, analysis, refreshed_timeframes)
            self._print_consistency_summary(consistency)

            instrument_result = self._build_instrument_cycle_result(
                symbol=symbol,
                analysis=analysis,
                consistency=consistency,
                refreshed_timeframes=refreshed_timeframes,
                symbol_state=symbol_state,
                tracker_result=tracker_result,
            )

            symbol_logger.info(
                f"Symbol analyzed. batch_group={self.batch_group} final_signal={instrument_result.final_signal} watch_status={instrument_result.watch_status}"
            )

            return instrument_result

        except Exception as error:
            fallback_journal_payload = {
                "status": "error",
                "error": str(error),
                "batch_group": self.batch_group,
            }
            fallback_snapshot_payload = self._build_fallback_snapshot_record(
                symbol=symbol.value,
                cycle_id=cycle_id,
                batch_id=batch_id,
                payload={
                    "instrument": symbol.value,
                    "timeframe": "15m",
                    "price": None,
                    "market_state": None,
                    "htf_bias": None,
                    "phase": None,
                    "context": {},
                    "setups": {},
                    "scenario": {"type": "SYMBOL_EXCEPTION"},
                    "final_signal": {"status": "ERROR", "direction": None, "confidence": 0.0},
                    "behavioral_summary": {"decision": "WAIT"},
                    "consistency": {},
                    "meta": {"error": str(error), "batch_group": self.batch_group},
                },
            )
            raise

        finally:
            try:
                self._safe_write_symbol_history(
                    symbol=symbol.value,
                    cycle_id=cycle_id,
                    batch_id=batch_id,
                    snapshot_payload=fallback_snapshot_payload,
                    journal_event="symbol_finished_fallback",
                    journal_payload=fallback_journal_payload,
                )
            except Exception as fallback_error:
                print(f"[FALLBACK FINAL WRITE ERROR] {symbol.value}: {fallback_error}")

    def _build_tracker_source_payload(
        self,
        *,
        source: Any,
        symbol: str,
        batch_group: str,
        cycle_id: str,
        price: float | None,
        market_state: str | None,
        htf_bias: str | None,
        phase: str | None,
        scenario_type: str | None,
        scenario_probability: float | None,
        scenario_decision: str | None,
        scenario_engine_failed: bool = False,
        scenario_engine_error: str | None = None,
        scenario_engine_error_type: str | None = None,
    ) -> dict[str, Any]:
        """
        Build a tracker-safe payload with guaranteed symbol propagation.

        Scenario/final_signal objects can be neutral NO_ACTION objects without
        instrument metadata. If they go directly into SignalTracker, they can be
        normalized as UNKNOWN and poison open_signals/statistics. The batch loop
        already knows the canonical symbol, so the runner makes it the source of
        truth before lifecycle tracking.
        """
        raw = to_jsonable(source or {})
        if not isinstance(raw, dict):
            raw = {"raw_source": raw}

        raw["symbol"] = symbol
        raw["instrument"] = symbol
        raw["cycle_id"] = cycle_id
        raw["batch_group"] = batch_group

        if raw.get("price") is None:
            raw["price"] = price
        if raw.get("market_state") is None:
            raw["market_state"] = market_state
        if raw.get("htf_bias") is None:
            raw["htf_bias"] = htf_bias
        if raw.get("phase") is None:
            raw["phase"] = phase

        if raw.get("scenario_type") is None and scenario_type is not None:
            raw["scenario_type"] = scenario_type
        if raw.get("scenario") is None and scenario_type is not None:
            raw["scenario"] = scenario_type
        if raw.get("alignment_score") is None and scenario_probability is not None:
            raw["alignment_score"] = scenario_probability
        if raw.get("decision") is None and scenario_decision is not None:
            raw["decision"] = scenario_decision

        metadata = raw.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.update(
            {
                "symbol": symbol,
                "instrument": symbol,
                "batch_group": batch_group,
                "cycle_id": cycle_id,
                "htf_bias": htf_bias,
                "market_state": market_state,
                "source": "stateful_batch_runner",
                "scenario_engine_failed": bool(scenario_engine_failed),
                "scenario_engine_error": scenario_engine_error,
                "scenario_engine_error_type": scenario_engine_error_type,
            }
        )
        raw["metadata"] = metadata

        return raw

    def _attach_tpo_policy_to_payload(
        self,
        *,
        payload: dict[str, Any],
        auction_context: dict[str, Any] | None,
        auction_filters: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """
        Attach TPO / auction context to a signal payload without changing
        lifecycle state.

        This method is intentionally defensive because the upstream TPO store
        and the downstream Battle Gate/statistics layer use slightly different
        field names. The live runner must preserve both contracts:

        - auction_filters.tpo_signal_permission / tpo_telegram_modifier for
          reporting/statistics;
        - payload-level tpo_signal_permission / tpo_telegram_modifier for
          Battle Gate and Telegram telemetry;
        - metadata.* mirrors for downstream normalizers that read metadata only.
        """
        if not isinstance(payload, dict):
            return {}

        if not isinstance(auction_context, dict):
            auction_context = {}
        if not isinstance(auction_filters, dict):
            auction_filters = {}

        # Work on copies so we do not mutate the analysis snapshot object
        # accidentally.
        context_payload = to_jsonable(auction_context)
        filters_payload = to_jsonable(auction_filters)
        if not isinstance(context_payload, dict):
            context_payload = {}
        if not isinstance(filters_payload, dict):
            filters_payload = {}

        def first_non_empty(*values: Any, default: Any = None) -> Any:
            for value in values:
                if value is None:
                    continue
                if isinstance(value, str) and value.strip() == "":
                    continue
                return value
            return default

        open_relation = str(
            first_non_empty(
                filters_payload.get("open_relation"),
                filters_payload.get("tpo_open_relation"),
                context_payload.get("open_relation"),
                context_payload.get("tpo_open_relation"),
                default="UNKNOWN",
            )
        ).upper()

        auction_bias = str(
            first_non_empty(
                filters_payload.get("auction_bias"),
                filters_payload.get("tpo_auction_bias"),
                context_payload.get("auction_bias"),
                context_payload.get("tpo_auction_bias"),
                default="UNKNOWN",
            )
        ).upper()

        market_status = str(
            first_non_empty(
                filters_payload.get("market_status"),
                filters_payload.get("market_state"),
                context_payload.get("market_status"),
                context_payload.get("market_state"),
                default="UNKNOWN",
            )
        ).upper()

        market_holiday_name = first_non_empty(
            filters_payload.get("market_holiday_name"),
            context_payload.get("market_holiday_name"),
        )

        modifier = str(
            first_non_empty(
                filters_payload.get("tpo_telegram_modifier"),
                filters_payload.get("telegram_modifier"),
                filters_payload.get("modifier"),
                context_payload.get("tpo_telegram_modifier"),
                context_payload.get("telegram_modifier"),
                default="NEUTRAL",
            )
        ).upper()

        is_stale = bool(filters_payload.get("is_stale") or context_payload.get("is_stale"))
        available = bool(
            filters_payload.get("auction_context_available")
            or context_payload.get("auction_context_available")
        )

        store_permission = first_non_empty(
            filters_payload.get("tpo_signal_permission"),
            filters_payload.get("signal_permission"),
            filters_payload.get("permission"),
            context_payload.get("tpo_signal_permission"),
            context_payload.get("signal_permission"),
            context_payload.get("permission"),
        )

        if store_permission is not None:
            permission = str(store_permission).upper()
            reason = str(
                first_non_empty(
                    filters_payload.get("tpo_signal_reason"),
                    filters_payload.get("signal_reason"),
                    filters_payload.get("reason"),
                    context_payload.get("tpo_signal_reason"),
                    context_payload.get("signal_reason"),
                    context_payload.get("reason"),
                    default="tpo_permission_from_offline_store",
                )
            )
        else:
            # Fallback for legacy TPO store snapshots that do not yet contain
            # explicit tpo_signal_permission.
            permission = "NEUTRAL"
            reason = "tpo_neutral_or_unavailable"

            if market_status in {"MARKET_CLOSED", "CLOSED"}:
                permission = "MARKET_CLOSED"
                reason = "tpo_market_closed"
            elif market_status == "STALE_DATA" or is_stale:
                permission = "STALE_DATA"
                reason = "tpo_stale_data"
            elif not available:
                permission = "NO_DATA"
                reason = "tpo_context_unavailable"
            elif TPO_SIGNAL_GATE_ENABLED:
                if open_relation == "INSIDE_VA" or auction_bias == "BALANCE" or modifier == "DOWNGRADE":
                    permission = "RESEARCH_ONLY"
                    reason = "tpo_blocks_battle_signal_inside_value_or_balance"
                elif open_relation in {"OUT_OF_RANGE", "RANGE"}:
                    permission = "OPEN_FOR_EVALUATION"
                    reason = "tpo_open_for_evaluation"
                else:
                    permission = "OPEN_FOR_EVALUATION"
                    reason = "tpo_context_available"

        # -----------------------------------------------------------------
        # NY market status guard
        # -----------------------------------------------------------------
        # Some offline TPO snapshots may carry stale MARKET_CLOSED state into
        # active NY / US / post-news reporting context. For NAS100/SPX500/UKOIL
        # this must not become a false MARKET_CLOSED block.
        #
        # If the context is stale, downgrade to STALE_DATA.
        # If the context is fresh enough, allow OPEN_FOR_EVALUATION.
        # This does not force a Telegram/Battle trade; it only fixes the reason.
        symbol_upper = str(
            payload.get("symbol")
            or payload.get("instrument")
            or filters_payload.get("symbol")
            or context_payload.get("symbol")
            or ""
        ).upper()

        session_haystack = " ".join(
            str(x or "")
            for x in (
                payload.get("report_type"),
                payload.get("session_label"),
                payload.get("session"),
                payload.get("session_anchor"),
                payload.get("market_session"),
                payload.get("batch_group"),
                filters_payload.get("report_type"),
                filters_payload.get("session_label"),
                filters_payload.get("session"),
                filters_payload.get("session_anchor"),
                filters_payload.get("market_session"),
                context_payload.get("report_type"),
                context_payload.get("session_label"),
                context_payload.get("session"),
                context_payload.get("session_anchor"),
                context_payload.get("market_session"),
                context_payload.get("current_session_id"),
            )
        ).upper()

        is_ny_context = any(
            token in session_haystack
            for token in RUNNER_NY_MARKET_STATUS_GUARD_TOKENS
        )

        original_market_status = market_status
        original_permission = permission

        ny_market_status_override = (
            RUNNER_NY_MARKET_STATUS_GUARD_ENABLED
            and symbol_upper in RUNNER_NY_MARKET_STATUS_GUARD_SYMBOLS
            and is_ny_context
            and (
                market_status in RUNNER_CLOSED_MARKET_STATUSES
                or str(permission or "").upper() in RUNNER_CLOSED_MARKET_STATUSES
            )
        )

        if ny_market_status_override:
            if is_stale:
                market_status = "STALE_DATA"
                permission = "STALE_DATA"
                reason = "ny_context_guard_converted_false_market_closed_to_stale_data"
                if modifier == "NEUTRAL":
                    modifier = "DOWNGRADE"
            else:
                market_status = "OPEN"
                permission = "OPEN_FOR_EVALUATION"
                reason = "ny_context_guard_converted_false_market_closed_to_open_for_evaluation"

            for target in (filters_payload, context_payload, payload):
                target["market_status_override"] = "NY_CONTEXT_MARKET_STATUS_GUARD"
                target["market_status_original"] = original_market_status
                target["tpo_signal_permission_original"] = original_permission
                target["market_status_override_reason"] = reason

        confidence_modifier = first_non_empty(
            filters_payload.get("confidence_modifier"),
            context_payload.get("confidence_modifier"),
            default=0.0,
        )

        auction_context_score = first_non_empty(
            filters_payload.get("auction_context_score"),
            context_payload.get("auction_context_score"),
            payload.get("auction_context_score"),
            default=0.0,
        )

        # Normalize the nested filters contract as well. Downstream telemetry
        # reads both metadata.auction_filters.* and payload-level fields.
        filters_payload["auction_context_available"] = available
        filters_payload["open_relation"] = open_relation
        filters_payload["auction_bias"] = auction_bias
        filters_payload["market_status"] = market_status
        filters_payload["tpo_signal_permission"] = permission
        filters_payload["tpo_telegram_modifier"] = modifier
        filters_payload["telegram_modifier"] = modifier
        filters_payload["confidence_modifier"] = confidence_modifier
        filters_payload["auction_context_score"] = auction_context_score
        filters_payload["is_stale"] = is_stale
        filters_payload.setdefault("tpo_source", context_payload.get("tpo_source", "offline_store"))

        context_payload["auction_context_available"] = available
        context_payload["open_relation"] = open_relation
        context_payload["auction_bias"] = auction_bias
        context_payload["market_status"] = market_status
        context_payload["tpo_signal_permission"] = permission
        context_payload["tpo_telegram_modifier"] = modifier
        context_payload["telegram_modifier"] = modifier
        context_payload["auction_context_score"] = auction_context_score
        context_payload["is_stale"] = is_stale
        context_payload.setdefault("tpo_source", filters_payload.get("tpo_source", "offline_store"))

        if market_holiday_name:
            filters_payload["market_holiday_name"] = market_holiday_name
            context_payload["market_holiday_name"] = market_holiday_name

        payload["auction_context"] = context_payload
        payload["auction_filters"] = filters_payload
        payload["auction_telemetry_mode"] = "offline_store_read_only"

        # Keep both old and new field names for compatibility.
        payload["open_relation"] = open_relation
        payload["auction_bias"] = auction_bias
        payload["market_status"] = market_status
        payload["auction_context_score"] = auction_context_score
        payload["tpo_open_relation"] = open_relation
        payload["tpo_auction_bias"] = auction_bias
        payload["tpo_telegram_modifier"] = modifier
        payload["tpo_confidence_modifier"] = confidence_modifier
        payload["tpo_signal_permission"] = permission
        payload["tpo_signal_reason"] = reason
        if market_holiday_name:
            payload["market_holiday_name"] = market_holiday_name

        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}

        metadata.update(
            {
                "auction_context": context_payload,
                "auction_filters": filters_payload,
                "auction_telemetry_mode": "offline_store_read_only",
                "open_relation": open_relation,
                "auction_bias": auction_bias,
                "market_status": market_status,
                "auction_context_score": auction_context_score,
                "tpo_open_relation": open_relation,
                "tpo_auction_bias": auction_bias,
                "tpo_telegram_modifier": modifier,
                "tpo_signal_permission": permission,
                "tpo_signal_reason": reason,
                "tpo_signal_gate_enabled": TPO_SIGNAL_GATE_ENABLED,
            }
        )
        if market_holiday_name:
            metadata["market_holiday_name"] = market_holiday_name

        payload["metadata"] = metadata

        try:
            payload = enrich_payload_with_tpo_watch(
                payload,
                context=context_payload,
                filters=filters_payload,
            )
        except Exception as error:  # noqa: BLE001
            logger.exception(
                "TPO Watch Bridge enrichment failed. symbol=%s error=%s",
                payload.get("symbol") or payload.get("instrument"),
                error,
            )
            metadata = payload.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
            metadata["tpo_watch_bridge_error"] = str(error)
            payload["metadata"] = metadata

        return payload


    def _enrich_tracker_payload_with_ltf_model_bridge(
        self,
        *,
        payload: dict[str, Any],
        series_by_tf: dict[Timeframe, pd.DataFrame],
    ) -> dict[str, Any]:
        """
        Convert active TPO Watch Bridge states into a live LTF model state.

        This is deliberately placed after _attach_tpo_policy_to_payload and before
        execution bridge / SignalTracker. It prevents OPEN_TEST_DRIVE contexts from
        being buried as NO_ACTION while still keeping Telegram silent until a real
        LTF model, stop, target and RR exist.
        """
        if not isinstance(payload, dict):
            return {}

        try:
            df_15m = series_by_tf.get(Timeframe.M15)
            enriched = enrich_payload_with_ltf_model(payload, df_15m=df_15m)

            metadata = enriched.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
                enriched["metadata"] = metadata

            metadata["ltf_model_bridge_status"] = enriched.get("ltf_model_state")
            metadata["ltf_model_bridge_reason"] = enriched.get("trigger_reason")
            metadata["runner_version"] = RUNNER_VERSION

            return enriched

        except Exception as error:  # noqa: BLE001
            logger.exception(
                "LTF model detector enrichment failed. symbol=%s error=%s",
                payload.get("symbol") or payload.get("instrument"),
                error,
            )
            metadata = payload.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
            metadata["ltf_model_detector_error"] = str(error)
            payload["metadata"] = metadata
            return payload


    def _enrich_tracker_payload_with_post_news_otd_bridge(
        self,
        *,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Add strict post-news OTD structural context for Battle/Macro Gate.

        This bridge does not open trades. It only converts an already confirmed
        OPEN_TEST_DRIVE + LTF model + real target + stop + >=3R practical RR
        into explicit acceptance/retest/ltf/target fields consumed by
        macro_event_guard. Missing one requirement keeps the payload as research.
        """
        if not isinstance(payload, dict):
            return {}

        try:
            enriched = apply_post_news_continuation(payload)

            metadata = enriched.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
                enriched["metadata"] = metadata

            metadata["post_news_otd_bridge_status"] = enriched.get("post_news_otd_model")
            metadata["post_news_otd_bridge_candidate"] = enriched.get("post_news_otd_candidate")
            metadata["runner_version"] = RUNNER_VERSION

            return enriched

        except Exception as error:  # noqa: BLE001
            logger.exception(
                "Post-news OTD bridge enrichment failed. symbol=%s error=%s",
                payload.get("symbol") or payload.get("instrument"),
                error,
            )
            metadata = payload.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
            metadata["post_news_otd_bridge_error"] = str(error)
            payload["metadata"] = metadata
            return payload


    def _enrich_tracker_payload_with_execution_bridge(
        self,
        *,
        payload: dict[str, Any],
        context: Any,
        scenario: Any,
        final_signal: Any,
    ) -> dict[str, Any]:
        """
        Conservative bridge from structural READY payloads to execution.py.

        It does NOT lower standards. It only calls build_execution_plan when:
        - payload status is READY;
        - direction is LONG/SHORT;
        - HTF is aligned, or HTF is NEUTRAL and TPO Watch Bridge marked
          OPEN_TEST_DRIVE as a valid transition candidate;
        - a canonical execution scenario can be inferred.

        If geometry/RR is incomplete, execution.py returns INCOMPLETE with a
        trigger_reason. Telegram remains blocked unless SignalTracker promotes
        the payload to READY with EXECUTABLE geometry.
        """
        if not isinstance(payload, dict):
            return {}

        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
            payload["metadata"] = metadata

        # -----------------------------------------------------------------
        # TPO/LTF detector guard
        # -----------------------------------------------------------------
        # The LTF detector is allowed to keep an OPEN_TEST_DRIVE context in
        # WATCH / LTF_MODEL_PENDING state. The old execution bridge must not
        # overwrite that useful state with generic reasons such as
        # "not_ready_status" or "neutral_or_invalid_direction".
        #
        # This guard keeps the context visible in journal/statistics:
        #   TPO_OPEN_TEST_DRIVE_WATCH
        #   LTF_MODEL_PENDING
        #   waiting_for_ltf_model_confirmation
        #
        # It does NOT create a trade signal and does NOT allow Telegram.
        ltf_model_state = str(
            payload.get("ltf_model_state")
            or metadata.get("ltf_model_state")
            or ""
        ).upper()

        ltf_model_state_full = str(
            payload.get("ltf_model_state_full")
            or metadata.get("ltf_model_state_full")
            or ""
        ).upper()

        tpo_watch_state = str(
            payload.get("tpo_watch_state")
            or metadata.get("tpo_watch_state")
            or ""
        ).upper()

        open_behavior = str(
            payload.get("open_behavior")
            or metadata.get("open_behavior")
            or ""
        ).upper()

        if (
            ltf_model_state == "PENDING"
            and tpo_watch_state == "LTF_MODEL_PENDING"
            and open_behavior == "OPEN_TEST_DRIVE"
        ):
            metadata["execution_bridge_status"] = "SKIPPED"
            metadata["execution_bridge_reason"] = "tpo_ltf_model_pending"
            metadata["execution_bridge_detail"] = (
                "OPEN_TEST_DRIVE is active, but LTF model is still pending; "
                "do not overwrite detector trigger_reason."
            )
            metadata["ltf_model_state"] = ltf_model_state
            metadata["ltf_model_state_full"] = ltf_model_state_full or "LTF_MODEL_PENDING"
            metadata["ltf_model_outcome"] = (
                payload.get("ltf_model_outcome")
                or metadata.get("ltf_model_outcome")
                or "PENDING_WAITING_FOR_LTF_MODEL_CONFIRMATION"
            )

            execution = payload.get("execution")
            if not isinstance(execution, dict):
                execution = {}

            trigger_reason = (
                payload.get("trigger_reason")
                or metadata.get("trigger_reason")
                or metadata.get("ltf_model_bridge_reason")
                or "waiting_for_ltf_model_confirmation"
            )

            execution.update(
                {
                    "status": "NOT_EXECUTABLE",
                    "model": "NONE",
                    "entry_reference_price": execution.get("entry_reference_price"),
                    "invalidation_reference_price": execution.get("invalidation_reference_price"),
                    "target_reference_price": execution.get("target_reference_price"),
                    "risk_reward_ratio": execution.get("risk_reward_ratio"),
                    "stop_distance": execution.get("stop_distance"),
                    "target_distance": execution.get("target_distance"),
                    "execution_timeframe": execution.get("execution_timeframe") or "15m",
                    "trigger_reason": trigger_reason,
                    "ltf_model_state": ltf_model_state,
                    "ltf_model_state_full": ltf_model_state_full or "LTF_MODEL_PENDING",
                    "ltf_model_outcome": metadata["ltf_model_outcome"],
                    "ltf_model_blockers": (
                        payload.get("ltf_model_blockers")
                        or metadata.get("ltf_model_blockers")
                        or []
                    ),
                }
            )

            payload["execution"] = execution
            payload["execution_status"] = "NOT_EXECUTABLE"
            payload["execution_model"] = "NONE"
            payload["trigger_reason"] = trigger_reason

            if not payload.get("scenario") or str(payload.get("scenario")).upper() == "NO_ACTION":
                payload["scenario"] = "TPO_OPEN_TEST_DRIVE_WATCH"
                payload["scenario_type"] = "TPO_OPEN_TEST_DRIVE_WATCH"

            if not payload.get("status") or str(payload.get("status")).upper() in {"NO_SETUP", "IDLE"}:
                payload["status"] = "WATCH"

            if not payload.get("signal_class"):
                payload["signal_class"] = "WATCH"
            if not payload.get("stage"):
                payload["stage"] = payload.get("signal_class") or "WATCH"

            if not payload.get("next_expected_event"):
                payload["next_expected_event"] = "ltf_model_confirmation"

            payload["metadata"] = metadata
            return payload

        # If ScenarioEngine already provided a real execution payload, keep it.
        existing_status = str(payload.get("execution_status") or "").upper()
        existing_model = str(payload.get("execution_model") or "").upper()
        if existing_status == "EXECUTABLE" or existing_model not in {"", "NONE"}:
            metadata["execution_bridge_status"] = "kept_existing_execution"
            metadata["execution_bridge_reason"] = "payload_already_has_execution"
            return payload

        status = str(payload.get("status") or "").upper()
        direction = str(payload.get("direction") or "").upper()
        htf_bias = str(payload.get("htf_bias") or metadata.get("htf_bias") or "").upper()

        if status != "READY":
            return self._mark_execution_bridge_block(
                payload,
                reason="not_ready_status",
                detail=f"status={status or '-'}",
            )

        if direction not in {"LONG", "SHORT"}:
            return self._mark_execution_bridge_block(
                payload,
                reason="neutral_or_invalid_direction",
                detail=f"direction={direction or '-'}",
            )

        if htf_bias == "NEUTRAL":
            tpo_watch_state = str(
                payload.get("tpo_watch_state")
                or metadata.get("tpo_watch_state")
                or ""
            ).upper()
            open_behavior = str(
                payload.get("open_behavior")
                or metadata.get("open_behavior")
                or ""
            ).upper()
            allowed_htf_neutral_transition = bool(
                payload.get("allowed_htf_neutral_transition")
                or metadata.get("allowed_htf_neutral_transition")
            )

            if not (
                allowed_htf_neutral_transition
                and open_behavior == "OPEN_TEST_DRIVE"
                and tpo_watch_state in {"LTF_MODEL_PENDING", "LTF_MODEL_CONFIRMED"}
            ):
                return self._mark_execution_bridge_block(
                    payload,
                    reason="neutral_htf_bias",
                    detail=(
                        "HTF bias is NEUTRAL; execution bridge requires "
                        "OPEN_TEST_DRIVE transition candidate from TPO Watch Bridge"
                    ),
                )

            metadata["execution_bridge_neutral_otd_allowed"] = True
            metadata["execution_bridge_neutral_otd_reason"] = (
                "OPEN_TEST_DRIVE + HTF NEUTRAL is treated as transition candidate"
            )

        canonical_scenario = self._infer_execution_bridge_scenario(payload)
        if canonical_scenario is None:
            return self._mark_execution_bridge_block(
                payload,
                reason="unsupported_bridge_scenario",
                detail=str(payload.get("scenario") or payload.get("scenario_type") or "-"),
            )

        try:
            plan = build_execution_plan(
                context=context,
                scenario_type=canonical_scenario,
                direction=direction,
                evidence=None,
            )
            execution_payload = self._execution_plan_to_payload(plan)
        except Exception as error:  # noqa: BLE001
            logger.exception(
                "Execution bridge failed. symbol=%s scenario=%s canonical=%s error=%s",
                payload.get("symbol"),
                payload.get("scenario"),
                canonical_scenario,
                error,
            )
            execution_payload = {
                "status": "INCOMPLETE",
                "model": "NONE",
                "entry_reference_price": None,
                "invalidation_reference_price": None,
                "target_reference_price": None,
                "risk_reward_ratio": None,
                "stop_distance": None,
                "target_distance": None,
                "execution_timeframe": None,
                "trigger_reason": f"execution_bridge_exception:{type(error).__name__}",
            }

        payload["execution"] = execution_payload
        payload["execution_status"] = execution_payload.get("status")
        payload["execution_model"] = execution_payload.get("model")
        payload["entry_reference_price"] = execution_payload.get("entry_reference_price")
        payload["invalidation_reference_price"] = execution_payload.get("invalidation_reference_price")
        payload["target_reference_price"] = execution_payload.get("target_reference_price")
        payload["risk_reward_ratio"] = execution_payload.get("risk_reward_ratio")
        payload["stop_distance"] = execution_payload.get("stop_distance")
        payload["target_distance"] = execution_payload.get("target_distance")
        payload["execution_timeframe"] = execution_payload.get("execution_timeframe")
        payload["trigger_reason"] = execution_payload.get("trigger_reason")

        metadata["execution_bridge_status"] = execution_payload.get("status")
        metadata["execution_bridge_model"] = execution_payload.get("model")
        metadata["execution_bridge_scenario"] = canonical_scenario
        metadata["execution_bridge_reason"] = execution_payload.get("trigger_reason")

        return payload

    def _mark_execution_bridge_block(
        self,
        payload: dict[str, Any],
        *,
        reason: str,
        detail: str | None = None,
    ) -> dict[str, Any]:
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
            payload["metadata"] = metadata

        metadata["execution_bridge_status"] = "SKIPPED"
        metadata["execution_bridge_reason"] = reason
        metadata["execution_bridge_detail"] = detail

        execution = payload.get("execution")
        if not isinstance(execution, dict):
            execution = {}

        execution.setdefault("status", "NOT_EXECUTABLE")
        execution.setdefault("model", "NONE")
        execution.setdefault("entry_reference_price", None)
        execution.setdefault("invalidation_reference_price", None)
        execution.setdefault("target_reference_price", None)
        execution.setdefault("risk_reward_ratio", None)
        execution.setdefault("stop_distance", None)
        execution.setdefault("target_distance", None)
        execution.setdefault("execution_timeframe", None)
        execution["trigger_reason"] = execution.get("trigger_reason") or reason

        payload["execution"] = execution
        payload["execution_status"] = execution.get("status")
        payload["execution_model"] = execution.get("model")
        payload["trigger_reason"] = execution.get("trigger_reason")
        return payload

    def _infer_execution_bridge_scenario(self, payload: dict[str, Any]) -> str | None:
        direction = str(payload.get("direction") or "").upper()
        if direction not in {"LONG", "SHORT"}:
            return None

        scenario = str(payload.get("scenario") or payload.get("scenario_type") or "").upper()
        setup_type = str(payload.get("setup_type") or "").upper()

        if (
            "TPO_OPEN_TEST_DRIVE" in scenario
            or "OPEN_TEST_DRIVE" in scenario
            or setup_type == "TPO_OPEN_TEST_DRIVE"
        ):
            # The detector normally supplies ready-to-use execution geometry.
            # This fallback maps OTD to the closest existing execution family if
            # a later normalization path strips that geometry.
            return f"SWEEP_RETURN_{direction}"

        if (
            "SWEEP_RETURN" in scenario
            or "RETURN_TO_VALUE" in scenario
            or setup_type == "SWEEP_RETURN_TO_VALUE"
        ):
            return f"SWEEP_RETURN_{direction}"

        if (
            "TREND_CONTINUATION" in scenario
            or "CONTINUATION" in scenario
            or "IMPULSE" in scenario
            or setup_type == "IMPULSE_PULLBACK_CONTINUATION"
        ):
            return f"TREND_CONTINUATION_{direction}"

        return None

    @staticmethod
    def _execution_plan_to_payload(plan: Any) -> dict[str, Any]:
        if plan is None:
            return {
                "status": "NOT_EXECUTABLE",
                "model": "NONE",
                "entry_reference_price": None,
                "invalidation_reference_price": None,
                "target_reference_price": None,
                "risk_reward_ratio": None,
                "stop_distance": None,
                "target_distance": None,
                "execution_timeframe": None,
                "trigger_reason": "execution_plan_none",
            }

        raw = to_jsonable(plan)
        if not isinstance(raw, dict):
            raw = {}

        return {
            "status": raw.get("status", "NOT_EXECUTABLE"),
            "model": raw.get("model", "NONE"),
            "entry_reference_price": raw.get("entry_reference_price"),
            "invalidation_reference_price": raw.get("invalidation_reference_price"),
            "target_reference_price": raw.get("target_reference_price"),
            "risk_reward_ratio": raw.get("risk_reward_ratio"),
            "stop_distance": raw.get("stop_distance"),
            "target_distance": raw.get("target_distance"),
            "execution_timeframe": raw.get("execution_timeframe"),
            "trigger_reason": raw.get("trigger_reason"),
        }

    def _force_payload_symbol(
        self,
        payload: dict[str, Any] | None,
        *,
        symbol: str,
        batch_group: str,
        cycle_id: str,
        htf_bias: str | None = None,
        market_state: str | None = None,
    ) -> dict[str, Any]:
        """Force payload-level symbol fields to match the batch symbol."""
        if not isinstance(payload, dict):
            return {}

        payload["symbol"] = symbol
        payload["instrument"] = symbol
        payload.setdefault("cycle_id", cycle_id)
        payload["batch_group"] = batch_group

        if htf_bias is not None and not payload.get("htf_bias"):
            payload["htf_bias"] = htf_bias
        if market_state is not None and not payload.get("market_state"):
            payload["market_state"] = market_state

        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.update(
            {
                "symbol": symbol,
                "instrument": symbol,
                "batch_group": batch_group,
                "cycle_id": cycle_id,
                "htf_bias": payload.get("htf_bias"),
                "market_state": payload.get("market_state"),
                "source": "stateful_batch_runner",
            }
        )
        payload["metadata"] = metadata
        return payload

    def _force_tracker_result_symbol(
        self,
        *,
        tracker_result: SignalTrackerResult,
        symbol: str,
        batch_group: str,
        cycle_id: str,
    ) -> SignalTrackerResult:
        """
        Defensive symbol guard after SignalTracker processing.

        This prevents accidental UNKNOWN propagation if a tracker normalization
        path receives a weak NO_ACTION/neutral payload.
        """
        current_htf_bias = None
        current_market_state = None
        if isinstance(tracker_result.payload, dict):
            current_htf_bias = tracker_result.payload.get("htf_bias")
            current_market_state = tracker_result.payload.get("market_state")

        tracker_result.payload = self._force_payload_symbol(
            tracker_result.payload,
            symbol=symbol,
            batch_group=batch_group,
            cycle_id=cycle_id,
            htf_bias=current_htf_bias,
            market_state=current_market_state,
        )

        if tracker_result.previous_payload is not None:
            tracker_result.previous_payload = self._force_payload_symbol(
                tracker_result.previous_payload,
                symbol=symbol,
                batch_group=batch_group,
                cycle_id=cycle_id,
                htf_bias=current_htf_bias,
                market_state=current_market_state,
            )

        if isinstance(tracker_result.signal_id, str) and tracker_result.signal_id.startswith("UNKNOWN_"):
            corrected_signal_id = tracker_result.payload.get("signal_id")
            if isinstance(corrected_signal_id, str) and not corrected_signal_id.startswith("UNKNOWN_"):
                tracker_result.payload["legacy_unknown_signal_id"] = tracker_result.signal_id
                tracker_result.signal_id = corrected_signal_id

        return tracker_result

    def _build_instrument_cycle_result(
        self,
        *,
        symbol: Instrument,
        analysis: dict[str, Any],
        consistency: Any,
        refreshed_timeframes: list[str],
        symbol_state: SymbolState,
        tracker_result: SignalTrackerResult | None,
    ) -> InstrumentCycleResult:
        context = analysis.get("context")
        scenario = analysis.get("scenario")
        final_signal = analysis.get("final_signal")
        setups = analysis.get("setups") or []

        price = extract_context_price(context)
        market_state = extract_context_market_state(context)
        htf_bias = extract_context_htf_bias(context)
        phase = classify_market_phase(context)

        signal_setup = self._safe_attr(final_signal, "setup_name")
        if signal_setup is None:
            signal_setup = self._safe_attr(final_signal, "setup_type")
        signal_setup = getattr(signal_setup, "value", signal_setup)

        if signal_setup in {"NO_ACTION", "MARKET_CLOSED", "NONE"}:
            signal_setup = None

        signal_status = self._safe_attr(final_signal, "status")
        signal_direction = self._safe_attr(final_signal, "direction")
        signal_confidence = self._safe_attr(final_signal, "confidence")

        signal_status = getattr(signal_status, "value", signal_status)
        signal_direction = getattr(signal_direction, "value", signal_direction)

        setup_result = self._find_setup_by_name(setups, signal_setup)
        setup_status = self._safe_attr(setup_result, "status")
        direction = self._safe_attr(setup_result, "direction")
        confidence = self._safe_attr(setup_result, "confidence")

        setup_status = getattr(setup_status, "value", setup_status)
        direction = getattr(direction, "value", direction)

        if confidence is None:
            confidence = signal_confidence

        watch_reason = self._safe_attr(setup_result, "rationale")
        invalidation_level = self._extract_invalidation_level(setup_result)
        target_zone = self._extract_target_zone(setup_result)

        scenario_type = None
        scenario_probability = None
        behavioral_summary = None

        if scenario is not None and not (
            isinstance(scenario, dict) and scenario.get("scenario_engine_failed")
        ):
            scenario_type = self._safe_attr(scenario, "scenario_type")
            scenario_type = getattr(scenario_type, "value", scenario_type)
            scenario_probability = self._safe_attr(scenario, "alignment_score")
            behavioral_summary = scenario_type
        else:
            scenario_type = infer_behavioral_scenario(context, setups)
            scenario_probability = infer_alignment_score(context, setups)
            behavioral_summary = scenario_type

        tracked_payload = tracker_result.payload if tracker_result is not None else {}
        previous_payload = tracker_result.previous_payload if tracker_result is not None else None
        tracked_stage = tracked_payload.get("signal_class", "SCENARIO_FORMING")
        final_signal_normalized = self._normalize_final_signal(tracked_stage)
        watch_status = self._derive_watch_status_from_stage(tracked_stage)

        alert_payload = self._build_alert_payload_from_tracker_payload(
            symbol=symbol.value,
            tracked_payload=tracked_payload,
            previous_payload=previous_payload,
            tracker_action=tracker_result.action if tracker_result is not None else "NOOP",
            cycle_id=tracked_payload.get("cycle_id"),
            current_price=price,
            market_state=market_state,
            htf_bias=htf_bias,
            watch_reason=watch_reason,
            scenario_probability=scenario_probability,
            invalidation_level=invalidation_level,
            target_zone=target_zone,
            paper_mode=self.simulation_mode,
            batch_group=self.batch_group,
        )

        return InstrumentCycleResult(
            symbol=symbol.value,
            status="ok",
            price=price,
            market_state=market_state,
            htf_bias=htf_bias,
            phase=phase,
            setup=signal_setup,
            setup_status=setup_status,
            direction=direction or signal_direction,
            confidence=confidence,
            scenario_type=scenario_type,
            scenario_probability=scenario_probability,
            final_signal=final_signal_normalized,
            watch_status=watch_status,
            watch_reason=watch_reason,
            behavioral_summary=behavioral_summary,
            invalidation_level=invalidation_level,
            target_zone=target_zone,
            execution_status=tracked_payload.get("execution_status"),
            execution_model=tracked_payload.get("execution_model"),
            risk_reward_ratio=tracked_payload.get("risk_reward_ratio"),
            entry_reference_price=tracked_payload.get("entry_reference_price"),
            invalidation_reference_price=tracked_payload.get("invalidation_reference_price"),
            target_reference_price=tracked_payload.get("target_reference_price"),
            refreshed_timeframes=refreshed_timeframes,
            consistency_ok=getattr(consistency, "is_consistent", None),
            consistency_score=getattr(consistency, "consistency_score", None),
            conflict_flags=list(getattr(consistency, "conflict_flags", []) or []),
            consistency_warnings=list(getattr(consistency, "warnings", []) or []),
            consistency_summary=getattr(consistency, "summary", None),
            data_status="cache_only" if not refreshed_timeframes else "mixed_or_api",
            analysis_snapshot=to_jsonable(symbol_state.analysis_snapshot),
            error_message=None,
            alert_payload=alert_payload,
        )

    def _load_timeframe(self, symbol: Instrument, timeframe: Timeframe) -> Any:
        try:
            if self.simulation_mode:
                try:
                    return self.loader.load_with_sanity(
                        instrument=symbol,
                        timeframe=timeframe,
                        use_cache_only=True,
                    )
                except TypeError:
                    pass

            return self.loader.load_with_sanity(
                instrument=symbol,
                timeframe=timeframe,
            )

        except AttributeError:
            if self.simulation_mode:
                raise RuntimeError(
                    f"Simulation mode requested but loader has no cache-only path for {symbol.value} {timeframe.value}"
                )
            result = self.loader.refresh_timeframe(
                instrument=symbol,
                timeframe=timeframe,
            )
            return result

        except Exception as error:
            text = str(error)
            if "run out of API credits" in text or "current minute" in text or "code=429" in text:
                raise TwelveDataRateLimitError(text) from error
            raise

    def _prepare_auction_dataframe(self, df: pd.DataFrame | None) -> pd.DataFrame | None:
        """
        Convert provider/cache OHLC data into the dataframe shape required by
        app.auction.profile_engine.

        profile_engine expects explicit columns:
        timestamp, open, high, low, close, volume(optional)

        Loader data may arrive with a DatetimeIndex, with a named timestamp column,
        or with provider-specific names. This adapter is intentionally defensive:
        if it cannot normalize the data, auction telemetry fails closed and does
        not affect the trading pipeline.
        """
        if df is None or df.empty:
            return None

        out = df.copy()

        lower_columns = {str(col).lower().strip(): col for col in out.columns}
        has_timestamp_col = any(
            key in lower_columns
            for key in ("timestamp", "datetime", "date", "time")
        )

        if not has_timestamp_col:
            index_name = out.index.name or "timestamp"
            out = out.reset_index()
            if index_name in out.columns:
                out = out.rename(columns={index_name: "timestamp"})
            elif "index" in out.columns:
                out = out.rename(columns={"index": "timestamp"})

        rename_map: dict[Any, str] = {}
        for col in out.columns:
            key = str(col).lower().strip()
            if key in {"timestamp", "datetime", "date", "time"}:
                rename_map[col] = "timestamp"
            elif key in {"open", "o"}:
                rename_map[col] = "open"
            elif key in {"high", "h"}:
                rename_map[col] = "high"
            elif key in {"low", "l"}:
                rename_map[col] = "low"
            elif key in {"close", "c"}:
                rename_map[col] = "close"
            elif key in {"volume", "vol", "v"}:
                rename_map[col] = "volume"

        out = out.rename(columns=rename_map)

        required = {"timestamp", "open", "high", "low", "close"}
        if not required.issubset(set(out.columns)):
            return None

        cols = ["timestamp", "open", "high", "low", "close"]
        if "volume" in out.columns:
            cols.append("volume")

        out = out[cols].copy()

        # Hard memory guard for passive TPO telemetry.
        # We only need recent auction context; full-history TPO belongs in an
        # offline audit/export job, not in the live worker.
        if AUCTION_MAX_BARS > 0 and len(out) > AUCTION_MAX_BARS:
            out = out.tail(AUCTION_MAX_BARS).copy()

        if "volume" not in out.columns:
            out["volume"] = 1.0

        return out

    def _build_passive_auction_payload(
        self,
        symbol: Instrument,
        series_by_tf: dict[Timeframe, pd.DataFrame],
    ) -> dict[str, Any]:
        """
        Read precomputed TPO / Market Profile telemetry from disk.

        IMPORTANT:
        The live worker must NOT calculate profiles. TPO calculation is an
        offline/lightweight job. The live worker only reads the latest context
        and applies signal policy from that context. This prevents Render OOM.
        """
        del series_by_tf

        unavailable = {
            "context": {
                "auction_context_available": False,
                "tpo_source": "offline_store",
                "reason": "tpo_store_missing_or_symbol_not_found",
                "store_path": str(TPO_STORE_PATH),
            },
            "filters": {
                "auction_context_available": False,
                "open_relation": "UNKNOWN",
                "auction_bias": "UNKNOWN",
                "telegram_modifier": "NEUTRAL",
                "confidence_modifier": 0.0,
                "reasons": ["TPO context unavailable; live worker kept neutral."],
            },
            "telemetry_mode": "offline_store_read_only",
        }

        try:
            if not TPO_STORE_PATH.exists():
                return unavailable

            raw = json.loads(TPO_STORE_PATH.read_text(encoding="utf-8"))
            symbols = raw.get("symbols") if isinstance(raw, dict) else None
            if not isinstance(symbols, dict):
                return unavailable

            item = symbols.get(symbol.value)
            if not isinstance(item, dict):
                return unavailable

            context_payload = item.get("context")
            filters_payload = item.get("filters")
            if not isinstance(context_payload, dict):
                context_payload = {}
            if not isinstance(filters_payload, dict):
                filters_payload = {}

            updated_at = item.get("updated_at_utc") or raw.get("updated_at_utc")
            stale = False
            age_minutes = None
            if updated_at:
                try:
                    parsed = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
                    age_minutes = (datetime.now(UTC) - parsed.astimezone(UTC)).total_seconds() / 60.0
                    stale = age_minutes > TPO_MAX_STALE_MINUTES
                except Exception:
                    stale = True

            context_payload["auction_context_available"] = True
            context_payload["tpo_source"] = "offline_store"
            context_payload["updated_at_utc"] = updated_at
            context_payload["age_minutes"] = round(age_minutes, 2) if age_minutes is not None else None
            context_payload["is_stale"] = stale
            context_payload["store_path"] = str(TPO_STORE_PATH)

            filters_payload.setdefault("auction_context_available", True)
            filters_payload.setdefault("open_relation", context_payload.get("open_relation", "UNKNOWN"))
            filters_payload.setdefault("auction_bias", context_payload.get("auction_bias", "UNKNOWN"))
            filters_payload.setdefault("telegram_modifier", "NEUTRAL")
            filters_payload.setdefault("confidence_modifier", 0.0)
            filters_payload.setdefault("reasons", [])
            filters_payload["tpo_source"] = "offline_store"
            filters_payload["is_stale"] = stale

            if stale:
                filters_payload["telegram_modifier"] = "NEUTRAL"
                filters_payload["confidence_modifier"] = 0.0
                reasons = filters_payload.get("reasons")
                if not isinstance(reasons, list):
                    reasons = []
                reasons.append("TPO context is stale; live worker kept neutral.")
                filters_payload["reasons"] = reasons

            return {
                "context": to_jsonable(context_payload),
                "filters": to_jsonable(filters_payload),
                "telemetry_mode": "offline_store_read_only",
            }

        except Exception as auction_error:  # noqa: BLE001
            logger.exception(
                "TPO store read failed. symbol=%s error=%s",
                symbol.value,
                auction_error,
            )
            unavailable["context"]["auction_context_failed"] = True
            unavailable["context"]["error"] = str(auction_error)
            unavailable["filters"]["reasons"] = [
                f"TPO store read failed: {type(auction_error).__name__}"
            ]
            return unavailable


    def _run_analysis_pipeline(
        self,
        symbol: Instrument,
        series_by_tf: dict[Timeframe, pd.DataFrame],
    ) -> dict[str, Any]:
        context = self._build_context(symbol, series_by_tf)
        setups = self._run_setups(context)
        scenario = self._run_scenario_engine(context, setups)
        final_signal = self._select_final_signal(context, setups, scenario)
        auction_payload = self._build_passive_auction_payload(symbol, series_by_tf)

        return {
            "context": context,
            "setups": setups,
            "scenario": scenario,
            "final_signal": final_signal,
            "auction_context": auction_payload.get("context"),
            "auction_filters": auction_payload.get("filters"),
            "auction_telemetry_mode": auction_payload.get("telemetry_mode", "passive_only"),
        }

    def _build_context(
        self,
        symbol: Instrument,
        series_by_tf: dict[Timeframe, pd.DataFrame],
    ) -> Any:
        from app.context.context_builder import ContextBuilder, ContextBuilderInput

        required_tfs = [Timeframe.D1, Timeframe.H4, Timeframe.M30, Timeframe.M15]
        missing = [tf.value for tf in required_tfs if tf not in series_by_tf]
        if missing:
            raise RuntimeError(
                f"Missing required timeframes for context build: {', '.join(missing)}"
            )

        builder = ContextBuilder()
        payload = ContextBuilderInput(
            instrument=symbol,
            df_1d=series_by_tf[Timeframe.D1],
            df_4h=series_by_tf[Timeframe.H4],
            df_30m=series_by_tf[Timeframe.M30],
            df_15m=series_by_tf[Timeframe.M15],
        )
        return builder.build(payload)

    def _run_setups(self, context: Any) -> list[Any]:
        from app.context.schema import SetupAInput, SetupARule, SetupBInput, SetupBRule

        if not hasattr(context, "model_dump"):
            raise RuntimeError("Context is not a rich MarketContext model")

        setup_a_rule = SetupARule()
        setup_b_rule = SetupBRule()

        setup_a_result = setup_a_rule.evaluate(
            SetupAInput(
                context=context,
                config=setup_a_rule.config,
            )
        )

        setup_b_result = setup_b_rule.evaluate(
            SetupBInput(
                context=context,
                config=setup_b_rule.config,
            )
        )

        return [setup_a_result, setup_b_result]

    def _run_scenario_engine(self, context: Any, setups: list[Any]) -> Any:
        try:
            from app.scenarios.scenario_engine import ScenarioEngine

            engine = ScenarioEngine()
            return engine.run(context=context, setups=setups)

        except ModuleNotFoundError as error:
            logger.exception("ScenarioEngine module import failed: %s", error)
            return {
                "scenario_engine_failed": True,
                "scenario_engine_error": str(error),
                "scenario_engine_error_type": type(error).__name__,
                "scenario_engine_stage": "module_import",
            }

        except Exception as error:
            logger.exception("ScenarioEngine runtime failed: %s", error)
            return {
                "scenario_engine_failed": True,
                "scenario_engine_error": str(error),
                "scenario_engine_error_type": type(error).__name__,
                "scenario_engine_stage": "runtime",
            }

    def _select_final_signal(self, context: Any, setups: list[Any], scenario: Any) -> Any:
        del context

        if scenario is not None and not (
            isinstance(scenario, dict) and scenario.get("scenario_engine_failed")
        ):
            return scenario

        if not setups:
            return None

        def setup_priority(item: Any) -> tuple[int, int, int]:
            status = self._safe_attr(item, "status")
            grade = self._safe_attr(item, "grade")
            direction = self._safe_attr(item, "direction")

            status_value = getattr(status, "value", status)
            grade_value = getattr(grade, "value", grade)
            direction_value = getattr(direction, "value", direction)

            status_rank = {
                "TRIGGERED": 6,
                "ACTIVE": 5,
                "READY": 4,
                "BUILDING": 3,
                "RETURNING_TO_VALUE": 2,
                "PULLBACK_IN_PROGRESS": 2,
                "IMPULSE_FOUND": 1,
                "SWEEP_DETECTED": 1,
                "WATCH": 0,
                "IDLE": -1,
                "INVALID": -2,
                "INVALIDATED": -2,
                "COMPLETED": -3,
                "NO_SETUP": -4,
            }.get(status_value, -10)

            grade_rank = {
                "A": 3,
                "B": 2,
                "C": 1,
                None: 0,
            }.get(grade_value, 0)

            direction_rank = 0 if direction_value in (None, "NEUTRAL") else 1
            return status_rank, grade_rank, direction_rank

        ranked = sorted(setups, key=setup_priority, reverse=True)
        winner = ranked[0]

        status = self._safe_attr(winner, "status")
        status_value = getattr(status, "value", status)

        setup_type = self._safe_attr(winner, "setup_type")
        setup_name = getattr(setup_type, "value", setup_type)

        if status_value in {"IDLE", "NO_SETUP", None}:
            setattr(winner, "setup_name", None)
        else:
            setattr(winner, "setup_name", setup_name)

        return winner

    def _build_behavioral_journal_record(
        self,
        symbol: Instrument,
        context: Any,
        setups: list[Any],
        scenario: Any,
        final_signal: Any,
        refreshed_timeframes: list[str],
        auction_context: dict[str, Any] | None = None,
        auction_filters: dict[str, Any] | None = None,
        auction_telemetry_mode: str | None = "passive_only",
    ) -> dict[str, Any]:
        def _enum_value(value: Any) -> Any:
            return getattr(value, "value", value)

        def _condition_names(items: list[Any]) -> list[str]:
            result = []
            for item in items or []:
                name = self._safe_attr(item, "name")
                if name is not None:
                    result.append(str(name))
            return result

        def _setup_payload(setup: Any) -> dict[str, Any]:
            diagnostics = self._safe_attr(setup, "diagnostics")
            passed = self._safe_attr(diagnostics, "passed_conditions") or []
            failed = self._safe_attr(diagnostics, "failed_conditions") or []

            setup_type = self._safe_attr(setup, "setup_type")
            status = self._safe_attr(setup, "status")
            direction = self._safe_attr(setup, "direction")
            grade = self._safe_attr(setup, "grade")

            return {
                "name": _enum_value(setup_type),
                "status": _enum_value(status),
                "direction": _enum_value(direction),
                "confidence": self._safe_attr(setup, "confidence"),
                "grade": _enum_value(grade),
                "rationale": self._safe_attr(setup, "rationale"),
                "passed": _condition_names(passed),
                "failed": _condition_names(failed),
            }

        phase = classify_market_phase(context)
        market_state = extract_context_market_state(context)
        htf_bias = extract_context_htf_bias(context)
        price = extract_context_price(context)

        if not isinstance(auction_context, dict):
            auction_context = {
                "auction_context_available": False,
                "reason": "auction_context_not_provided",
            }

        if not isinstance(auction_filters, dict):
            auction_filters = {
                "auction_context_available": False,
                "open_relation": "UNKNOWN",
                "auction_bias": "UNKNOWN",
                "telegram_modifier": "NEUTRAL",
                "confidence_modifier": 0.0,
                "reasons": ["Auction filters unavailable."],
            }

        auction_telemetry_mode = auction_telemetry_mode or "passive_only"

        setup_a = setups[0] if len(setups) > 0 else None
        setup_b = setups[1] if len(setups) > 1 else None

        final_status = _enum_value(self._safe_attr(final_signal, "status"))
        final_direction = _enum_value(self._safe_attr(final_signal, "direction"))
        final_setup = self._safe_attr(final_signal, "setup_name")
        if final_setup is None:
            final_setup = _enum_value(self._safe_attr(final_signal, "setup_type"))

        scenario_ok = scenario is not None and not (
            isinstance(scenario, dict) and scenario.get("scenario_engine_failed")
        )

        if scenario_ok:
            dominant_scenario = _enum_value(self._safe_attr(scenario, "scenario_type"))
            next_expected_event = self._safe_attr(scenario, "next_expected_event")
            alignment_score = self._safe_attr(scenario, "alignment_score")
            missing_conditions = self._safe_attr(scenario, "missing_conditions") or []
            scenario_phase = _enum_value(self._safe_attr(scenario, "phase"))
            scenario_decision = _enum_value(self._safe_attr(scenario, "decision"))
            scenario_evidence = self._safe_attr(scenario, "evidence")
            execution = self._safe_attr(scenario, "execution")
        else:
            dominant_scenario = infer_behavioral_scenario(context, setups)
            next_expected_event = infer_next_expected_event(context, setups)
            alignment_score = infer_alignment_score(context, setups)
            missing_conditions = infer_missing_conditions(setups)
            scenario_phase = None
            scenario_decision = None
            scenario_evidence = None
            execution = None

        return {
            "ts": now_iso(),
            "instrument": symbol.value,
            "batch_group": self.batch_group,
            "price": price,
            "market_state": market_state,
            "htf_bias": htf_bias,
            "phase": phase,
            "context": {
                "acceptance": {
                    "accepted_above": self._safe_attr(self._safe_attr(context, "acceptance"), "accepted_above"),
                    "accepted_below": self._safe_attr(self._safe_attr(context, "acceptance"), "accepted_below"),
                    "no_acceptance_above": self._safe_attr(self._safe_attr(context, "acceptance"), "no_acceptance_above"),
                    "no_acceptance_below": self._safe_attr(self._safe_attr(context, "acceptance"), "no_acceptance_below"),
                },
                "structure_4h": {
                    "bos_up": self._safe_attr(self._safe_attr(context, "structure_4h"), "bos_up"),
                    "bos_down": self._safe_attr(self._safe_attr(context, "structure_4h"), "bos_down"),
                    "hh_hl_structure": self._safe_attr(self._safe_attr(context, "structure_4h"), "hh_hl_structure"),
                    "ll_lh_structure": self._safe_attr(self._safe_attr(context, "structure_4h"), "ll_lh_structure"),
                },
                "structure_15m": {
                    "bos_up": self._safe_attr(self._safe_attr(context, "structure_15m"), "bos_up"),
                    "bos_down": self._safe_attr(self._safe_attr(context, "structure_15m"), "bos_down"),
                    "hh_hl_structure": self._safe_attr(self._safe_attr(context, "structure_15m"), "hh_hl_structure"),
                    "ll_lh_structure": self._safe_attr(self._safe_attr(context, "structure_15m"), "ll_lh_structure"),
                },
                "impulse": to_jsonable(self._safe_attr(context, "impulse")),
                "pullback": to_jsonable(self._safe_attr(context, "pullback")),
                "sweep": to_jsonable(self._safe_attr(context, "sweep")),
                "profile": {
                    "monthly": to_jsonable(self._safe_attr(context, "monthly_profile")),
                    "weekly": to_jsonable(self._safe_attr(context, "weekly_profile")),
                    "daily": to_jsonable(self._safe_attr(context, "daily_profile")),
                },
                "auction": {
                    "context": to_jsonable(auction_context),
                    "filters": to_jsonable(auction_filters),
                    "telemetry_mode": auction_telemetry_mode,
                },
            },
            "setups": {
                "setup_a": _setup_payload(setup_a) if setup_a else None,
                "setup_b": _setup_payload(setup_b) if setup_b else None,
            },
            "scenario": {
                "type": dominant_scenario if scenario_ok else None,
                "phase": scenario_phase,
                "decision": scenario_decision,
                "next_expected_event": next_expected_event,
                "missing_conditions": missing_conditions,
                "alignment_score": alignment_score,
                "evidence": (
                    scenario_evidence.model_dump()
                    if hasattr(scenario_evidence, "model_dump")
                    else to_jsonable(scenario_evidence)
                ),
                "execution": (
                    execution.model_dump()
                    if hasattr(execution, "model_dump")
                    else to_jsonable(execution)
                ),
            },
            "final_signal": {
                "setup": final_setup,
                "status": final_status,
                "direction": final_direction,
                "confidence": self._safe_attr(final_signal, "confidence"),
            },
            "behavioral_summary": {
                "dominant_scenario": dominant_scenario,
                "decision": (
                    scenario_decision
                    if scenario_decision is not None
                    else ("NO_TRADE" if final_status in {"IDLE", "NO_SETUP", None} else "WATCH_OR_TRADE")
                ),
                "missing_conditions": missing_conditions,
                "next_expected_event": next_expected_event,
                "alignment_score": alignment_score,
                "auction_note": (
                    auction_filters.get("reasons", [None])[0]
                    if isinstance(auction_filters.get("reasons"), list)
                    and auction_filters.get("reasons")
                    else None
                ),
                "auction_bias": auction_context.get("auction_bias"),
                "open_relation": auction_context.get("open_relation"),
                "auction_telegram_modifier": auction_filters.get("telegram_modifier"),
                "auction_confidence_modifier": auction_filters.get("confidence_modifier"),
                "auction_telemetry_mode": auction_telemetry_mode,
            },
            "auction_context": to_jsonable(auction_context),
            "auction_filters": to_jsonable(auction_filters),
            "auction_telemetry_mode": auction_telemetry_mode,
            "meta": {
                "simulation_mode": self.simulation_mode,
                "batch_group": self.batch_group,
                "refreshed_timeframes": refreshed_timeframes,
                "data_source": "cache_only" if not refreshed_timeframes else "mixed_or_api",
                "auction_telemetry_mode": auction_telemetry_mode,
            },
        }

    def _build_consistency_payload(
        self,
        context: Any,
        setups: list[Any],
        final_signal: Any,
    ) -> dict[str, Any]:
        def _enum_value(value: Any) -> Any:
            return getattr(value, "value", value)

        def _extract_setup_diag(setup: Any) -> dict[str, Any]:
            if setup is None:
                return {
                    "status": None,
                    "direction": None,
                    "confidence": 0.0,
                    "passed": [],
                    "failed": [],
                }

            diagnostics = self._safe_attr(setup, "diagnostics")
            passed = self._safe_attr(diagnostics, "passed_conditions") or []
            failed = self._safe_attr(diagnostics, "failed_conditions") or []

            return {
                "status": _enum_value(self._safe_attr(setup, "status")),
                "direction": _enum_value(self._safe_attr(setup, "direction")),
                "confidence": self._safe_attr(setup, "confidence") or 0.0,
                "passed": [
                    str(self._safe_attr(item, "name"))
                    for item in passed
                    if self._safe_attr(item, "name") is not None
                ],
                "failed": [
                    str(self._safe_attr(item, "name"))
                    for item in failed
                    if self._safe_attr(item, "name") is not None
                ],
            }

        setup_a = setups[0] if len(setups) > 0 else None
        setup_b = setups[1] if len(setups) > 1 else None

        final_setup = self._safe_attr(final_signal, "setup_name")
        if final_setup is None:
            final_setup = _enum_value(self._safe_attr(final_signal, "setup_type"))

        return {
            "market_state": extract_context_market_state(context),
            "htf_bias": extract_context_htf_bias(context),
            "phase": classify_market_phase(context),
            "final_signal_setup": final_setup,
            "final_signal_status": _enum_value(self._safe_attr(final_signal, "status")),
            "final_signal_direction": _enum_value(self._safe_attr(final_signal, "direction")),
            "behavioral_summary": None,
            "diagnostics": {
                "IMPULSE_PULLBACK_CONTINUATION": _extract_setup_diag(setup_a),
                "SWEEP_RETURN_TO_VALUE": _extract_setup_diag(setup_b),
            },
        }

    def _print_consistency_summary(self, consistency: Any) -> None:
        print("\n  CONSISTENCY CHECK")
        print(f"    OK:          {getattr(consistency, 'is_consistent', None)}")
        print(f"    Score:       {getattr(consistency, 'consistency_score', None)}")
        print(f"    Summary:     {getattr(consistency, 'summary', None)}")

        conflict_flags = getattr(consistency, "conflict_flags", []) or []
        warnings = getattr(consistency, "warnings", []) or []

        if conflict_flags:
            print("    CONFLICTS:")
            for flag in conflict_flags:
                print(f"      ❌ {flag}")

        if warnings:
            print("    WARNINGS:")
            for warning in warnings:
                print(f"      ⚠️ {warning}")

    def _ensure_budget_or_wait(self, credits: int) -> None:
        if self.budget.can_spend(credits):
            return

        wait_seconds = self.budget.seconds_until_reset()
        if wait_seconds > 0:
            print(f"[RATE LIMIT] waiting {wait_seconds:.1f}s for next minute window")
            time.sleep(wait_seconds + 0.5)

    def _ensure_batch_initialized(self, *, batch_index: int, current_symbols: list[str]) -> None:
        batch_changed = (
            self.state.current_batch_symbols != current_symbols
            or self.state.batch_index != batch_index
            or not self.state.current_batch_started_at
        )

        if batch_changed:
            self.state.batch_index = batch_index
            self.state.current_batch_symbols = current_symbols
            self.state.current_batch_started_at = now_iso()
            self.state.current_batch_completed_at = None
            self.state.symbol_states = {
                symbol: SymbolState(symbol=symbol)
                for symbol in current_symbols
            }
            self.state.last_run_status = "running"
            self.state.last_error = None
            save_state(self.state, self.state_path)

    def _advance_batch_pointer(self) -> None:
        next_index = self.state.batch_index + 1
        if next_index >= len(self.batches):
            next_index = 0

        self.state.batch_index = next_index
        self.state.current_batch_symbols = []
        self.state.current_batch_started_at = None
        self.state.current_batch_completed_at = None
        self.state.symbol_states = {}

    @staticmethod
    def _make_batches(items: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
        return [
            items[index:index + batch_size]
            for index in range(0, len(items), batch_size)
        ]

    def _print_header(self, batch_index: int, current_batch: list[dict[str, Any]]) -> None:
        print("========================================================================================")
        print("STATEFUL BATCH MULTI-INSTRUMENT ANALYTICS RUN")
        print("========================================================================================")
        print(f"BATCH GROUP:    {self.batch_group}")
        print(f"Batch size:      {self.state.batch_size}")
        print(f"Batch index:     {batch_index}")
        print(f"Total batches:   {self.state.total_batches}")
        print(f"AUTO_MODE:       {self.state.auto_mode}")
        print(f"FORCE_BATCH:     {self.state.force_batch}")
        print(f"SIMULATION:      {self.simulation_mode}")
        print("Інструменти в поточному batch:")
        for item in current_batch:
            print(f"  - {item['symbol'].value} (priority={item.get('priority', '-')})")
        print("========================================================================================")

    def _print_symbol_summary(
        self,
        symbol: Instrument,
        analysis: dict[str, Any],
        refreshed_timeframes: list[str],
    ) -> None:
        context = analysis.get("context")
        final_signal = analysis.get("final_signal")
        setups = analysis.get("setups") or []

        price = extract_context_price(context)
        market_state = extract_context_market_state(context)
        htf_bias = extract_context_htf_bias(context)
        phase = classify_market_phase(context)

        signal_setup = self._safe_attr(final_signal, "setup_name")
        signal_status = self._safe_attr(final_signal, "status")
        signal_direction = self._safe_attr(final_signal, "direction")

        signal_status = getattr(signal_status, "value", signal_status)
        signal_direction = getattr(signal_direction, "value", signal_direction)

        print(f"  Instrument:    {symbol.value}")
        print(f"  Price:         {price}")
        print(f"  Market state:  {market_state}")
        print(f"  HTF bias:      {htf_bias}")
        print(f"  Phase:         {phase}")
        print(f"  Refreshed TFs: {', '.join(refreshed_timeframes) if refreshed_timeframes else 'cache only'}")

        print("\n  FINAL SIGNAL")
        print(f"    Instrument:   {symbol.value}")
        print(f"    Setup:        {signal_setup}")
        print(f"    Status:       {signal_status}")
        print(f"    Direction:    {signal_direction}")

        if setups:
            print("\n  SETUP DIAGNOSTICS")
            for setup in setups:
                self._print_setup_debug(setup)

    def _print_setup_debug(self, result: Any) -> None:
        setup_type = self._safe_attr(result, "setup_type")
        setup_type = getattr(setup_type, "value", setup_type)

        status = self._safe_attr(result, "status")
        status = getattr(status, "value", status)

        direction = self._safe_attr(result, "direction")
        direction = getattr(direction, "value", direction)

        confidence = self._safe_attr(result, "confidence")
        rationale = self._safe_attr(result, "rationale")
        diagnostics = self._safe_attr(result, "diagnostics")

        passed = self._safe_attr(diagnostics, "passed_conditions") or []
        failed = self._safe_attr(diagnostics, "failed_conditions") or []

        print(f"    {setup_type}")
        print(f"      Status:      {status}")
        print(f"      Direction:   {direction}")
        print(f"      Confidence:  {confidence}")
        if rationale:
            print(f"      Rationale:   {rationale}")

        if passed:
            print("      PASSED:")
            for condition in passed:
                name = self._safe_attr(condition, "name")
                message = self._safe_attr(condition, "message")
                print(f"        ✅ {name}: {message}")

        if failed:
            print("      FAILED:")
            for condition in failed:
                name = self._safe_attr(condition, "name")
                message = self._safe_attr(condition, "message")
                print(f"        ❌ {name}: {message}")

    @staticmethod
    def _normalize_final_signal(stage: Any) -> str:
        value = str(stage or "").upper()
        if value == "ACTIVE":
            return "TRIGGERED"
        if value == "READY":
            return "READY"
        if value == "WATCH":
            return "WATCH"
        if value == "RESOLVED":
            return "INVALIDATED"
        return "IDLE"

    @staticmethod
    def _derive_watch_status_from_stage(stage: Any) -> str:
        value = str(stage or "").upper()
        if value == "ACTIVE":
            return "TRIGGERED"
        if value == "READY":
            return "NEW"
        if value == "WATCH":
            return "ACTIVE"
        if value == "RESOLVED":
            return "INVALIDATED"
        return "-"

    @staticmethod
    def _map_stage_to_alert_type(stage: str) -> str | None:
        stage = str(stage or "").upper()
        if stage == "ACTIVE":
            return "TRIGGERED"
        if stage == "READY":
            return "ENTRY_READY"
        if stage == "WATCH":
            return "WATCH_NEW"
        if stage == "RESOLVED":
            return "INVALIDATED"
        return None

    def _dispatch_alert_payload(self, payload: dict[str, Any]) -> bool:
        if not payload:
            return False

        if not payload.get("should_alert", False):
            block_reason = payload.get("telegram_block_reason") or "should_alert_false"
            logger.info(
                "Telegram skipped. symbol=%s signal_id=%s class=%s execution=%s rr=%s reason=%s",
                payload.get("symbol"),
                payload.get("signal_id"),
                payload.get("signal_class"),
                payload.get("execution_status"),
                payload.get("risk_reward_ratio"),
                block_reason,
            )
            return False

        hard_allowed, hard_reason = _is_telegram_trade_alert_allowed(payload)
        payload["telegram_hard_gate_allowed"] = hard_allowed
        payload["telegram_hard_gate_reason"] = hard_reason

        if not hard_allowed:
            payload["telegram_allowed"] = False
            payload["telegram_block_reason"] = hard_reason
            logger.info(
                "Telegram blocked by hard gate. symbol=%s signal_id=%s class=%s execution=%s rr=%s reason=%s",
                payload.get("symbol"),
                payload.get("signal_id"),
                payload.get("signal_class"),
                payload.get("execution_status"),
                payload.get("risk_reward_ratio"),
                hard_reason,
            )
            return False

        if payload.get("telegram_allowed") is not True:
            logger.info(
                "Telegram blocked by Signal Quality Engine. symbol=%s signal_id=%s class=%s execution=%s rr=%s reason=%s score=%s",
                payload.get("symbol"),
                payload.get("signal_id"),
                payload.get("signal_class"),
                payload.get("execution_status"),
                payload.get("risk_reward_ratio"),
                payload.get("signal_quality_reason"),
                payload.get("signal_quality_score"),
            )
            return False

        try:
            sent = self.telegram.send_alert_payload(payload)
            sent_at_utc = now_iso() if sent else None

            if sent and payload.get("signal_id"):
                self.signal_tracker.mark_alert_sent(
                    payload["signal_id"],
                    alert_type=payload.get("alert_type"),
                    sent_at_utc=sent_at_utc,
                )

            if sent:
                try:
                    payload["telegram_sent_at_utc"] = sent_at_utc
                    alert_snapshot = record_telegram_alert(
                        payload,
                        sent_at_utc=sent_at_utc,
                        source="stateful_batch_runner",
                    )
                    payload["telegram_alert_id"] = alert_snapshot.get("alert_id")

                    logger.info(
                        "Telegram alert snapshot recorded. symbol=%s alert_type=%s signal_id=%s alert_id=%s outcome=%s",
                        payload.get("symbol"),
                        payload.get("alert_type"),
                        payload.get("signal_id"),
                        alert_snapshot.get("alert_id"),
                        alert_snapshot.get("outcome_status"),
                    )

                except Exception as snapshot_error:  # noqa: BLE001
                    logger.exception(
                        "Telegram alert snapshot recording failed. symbol=%s alert_type=%s signal_id=%s error=%s",
                        payload.get("symbol"),
                        payload.get("alert_type"),
                        payload.get("signal_id"),
                        snapshot_error,
                    )

                logger.info(
                    "Telegram alert sent. symbol=%s alert_type=%s signal_id=%s rr=%s",
                    payload.get("symbol"),
                    payload.get("alert_type"),
                    payload.get("signal_id"),
                    payload.get("risk_reward_ratio"),
                )
            else:
                logger.warning(
                    "Telegram alert skipped or failed. symbol=%s alert_type=%s signal_id=%s",
                    payload.get("symbol"),
                    payload.get("alert_type"),
                    payload.get("signal_id"),
                )

            return sent

        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Telegram dispatch failed. symbol=%s alert_type=%s signal_id=%s error=%s",
                payload.get("symbol"),
                payload.get("alert_type"),
                payload.get("signal_id"),
                exc,
            )
            return False

    def _build_alert_payload_from_tracker_payload(
        self,
        *,
        symbol: str,
        tracked_payload: dict[str, Any],
        previous_payload: dict[str, Any] | None,
        tracker_action: str,
        cycle_id: str | None,
        current_price: float | None,
        market_state: str | None,
        htf_bias: str | None,
        watch_reason: str | None,
        scenario_probability: float | None,
        invalidation_level: float | None,
        target_zone: list[float] | None,
        paper_mode: bool,
        batch_group: str,
    ) -> Optional[dict[str, Any]]:
        if not tracked_payload:
            return None

        # Send only lifecycle changes. Do not re-alert unchanged open signals.
        if str(tracker_action or "").upper() not in {"REGISTERED", "UPDATED"}:
            return None

        # If this exact signal was already delivered, keep Telegram silent.
        if tracked_payload.get("was_sent_to_telegram") is True:
            return None

        signal_class = str(tracked_payload.get("signal_class") or "").upper()
        execution_status = str(tracked_payload.get("execution_status") or "").upper()
        scenario = str(tracked_payload.get("scenario") or "").upper()
        direction = str(tracked_payload.get("direction") or "").upper()

        previous_signal_class = str((previous_payload or {}).get("signal_class") or "").upper()
        previous_execution_status = str((previous_payload or {}).get("execution_status") or "").upper()

        # Telegram must be silent for reconnaissance states.
        # These states are still tracked and written to journal/statistics above.
        if signal_class != "READY":
            return None

        if execution_status != "EXECUTABLE":
            return None

        if scenario in {"", "NO_ACTION", "MARKET_CLOSED"}:
            return None

        if direction not in {"LONG", "SHORT"}:
            return None

        trigger_reason = str(tracked_payload.get("trigger_reason") or "")
        if "invalid_geometry" in trigger_reason.lower():
            return None

        entry_price = tracked_payload.get("entry_reference_price")
        stop_price = tracked_payload.get("invalidation_reference_price")
        target_price = tracked_payload.get("target_reference_price")
        rr = (
            tracked_payload.get("risk_reward_ratio")
            or tracked_payload.get("practical_rr")
            or tracked_payload.get("rr")
            or tracked_payload.get("rr_ratio")
            or tracked_payload.get("risk_reward")
            or tracked_payload.get("expected_rr")
            or tracked_payload.get("planned_rr")
        )

        tpo_signal_permission = str(tracked_payload.get("tpo_signal_permission") or "NEUTRAL").upper()
        tpo_signal_reason = tracked_payload.get("tpo_signal_reason")
        tpo_open_relation = tracked_payload.get("tpo_open_relation")
        tpo_auction_bias = tracked_payload.get("tpo_auction_bias")
        tpo_telegram_modifier = tracked_payload.get("tpo_telegram_modifier")

        alert_current_price = (
            tracked_payload.get("current_price")
            or tracked_payload.get("last_price")
            or current_price
        )

        raw_alert_payload = {
            "schema_version": "2.0",
            "should_alert": True,
            "alert_type": "ENTRY_READY",
            "signal_id": tracked_payload.get("signal_id"),
            "symbol": symbol,
            "batch_group": batch_group,
            "cycle_id": cycle_id,
            "current_price": alert_current_price,
            "last_price": alert_current_price,
            "paper_mode": paper_mode,
            "signal_class": tracked_payload.get("signal_class"),
            "stage": tracked_payload.get("signal_class"),
            "current_stage": tracked_payload.get("signal_class"),
            "scenario": tracked_payload.get("scenario"),
            "scenario_type": tracked_payload.get("scenario"),
            "direction": tracked_payload.get("direction"),
            "confidence": tracked_payload.get("confidence"),
            "probability": tracked_payload.get("confidence"),
            "scenario_probability": scenario_probability,
            "alignment_score": tracked_payload.get("alignment_score"),
            "market_state": market_state,
            "htf_bias": htf_bias,
            "phase": tracked_payload.get("phase"),
            "status": tracked_payload.get("status"),
            "watch_reason": tracked_payload.get("rationale") or watch_reason,
            "reason": tracked_payload.get("rationale") or watch_reason,
            "next_expected_event": tracked_payload.get("next_expected_event"),
            "missing_conditions": tracked_payload.get("missing_conditions") or [],
            "tags": tracked_payload.get("tags") or [],
            "execution_status": tracked_payload.get("execution_status"),
            "execution_model": tracked_payload.get("execution_model"),
            "execution_timeframe": tracked_payload.get("execution_timeframe"),
            "trigger_reason": tracked_payload.get("trigger_reason"),
            "entry_reference_price": entry_price,
            "invalidation_reference_price": stop_price,
            "target_reference_price": target_price,
            "risk_reward_ratio": rr,
            "practical_rr": rr,
            "entry": entry_price,
            "stop_loss": stop_price,
            "take_profit": target_price,
            "rr": rr,
            "stop_distance": tracked_payload.get("stop_distance"),
            "target_distance": tracked_payload.get("target_distance"),
            "invalidation_level": invalidation_level if invalidation_level is not None else stop_price,
            "target_zone": target_zone if target_zone else ([target_price] if target_price is not None else []),
            "previous_signal_class": previous_signal_class or None,
            "previous_execution_status": previous_execution_status or None,
            "tracker_action": tracker_action,
            "auction_context": tracked_payload.get("auction_context"),
            "auction_filters": tracked_payload.get("auction_filters"),
            "auction_telemetry_mode": tracked_payload.get("auction_telemetry_mode"),
            "tpo_open_relation": tpo_open_relation,
            "tpo_auction_bias": tpo_auction_bias,
            "tpo_telegram_modifier": tpo_telegram_modifier,
            "tpo_signal_permission": tpo_signal_permission,
            "tpo_signal_reason": tpo_signal_reason,
            "open_context": tracked_payload.get("open_context"),
            "open_behavior": tracked_payload.get("open_behavior"),
            "open_behavior_confidence": tracked_payload.get("open_behavior_confidence"),
            "entry_model_hint": tracked_payload.get("entry_model_hint"),
            "stop_model_hint": tracked_payload.get("stop_model_hint"),
            "battle_bias_hint": tracked_payload.get("battle_bias_hint"),
            "tpo_watch_state": tracked_payload.get("tpo_watch_state"),
            "ltf_model_state": tracked_payload.get("ltf_model_state"),
            "tpo_watch_active": tracked_payload.get("tpo_watch_active"),
            "tpo_watch_setup": tracked_payload.get("tpo_watch_setup"),
            "tpo_watch_reason": tracked_payload.get("tpo_watch_reason"),
            "allowed_htf_neutral_transition": tracked_payload.get("allowed_htf_neutral_transition"),
            "htf_alignment_state": tracked_payload.get("htf_alignment_state"),
            "primary_interest_zone": tracked_payload.get("primary_interest_zone"),
            "interest_zone_type": tracked_payload.get("interest_zone_type"),
            "interest_zone_price": tracked_payload.get("interest_zone_price"),
            "interest_zone_role": tracked_payload.get("interest_zone_role"),
        }

        tracked_metadata = tracked_payload.get("metadata") if isinstance(tracked_payload.get("metadata"), dict) else {}
        for key in (
            "ltf_model_state_full",
            "ltf_model_outcome",
            "ltf_model_type",
            "ltf_model_confirmed",
            "ltf_model_reasons",
            "ltf_model_blockers",
            "target_source",
            "target_quality",
            "target_zone_type",
            "target_zone_role",
            "acceptance_confirmed",
            "post_news_acceptance_confirmed",
            "retest_confirmed",
            "post_news_retest_confirmed",
            "ltf_confirmed",
            "real_target",
            "has_real_target",
            "stop_ok",
            "practical_rr_ok",
            "post_news_detector_version",
            "post_news_regime",
            "post_news_trade_permission",
            "post_news_elapsed_minutes",
            "post_news_impulse_direction",
            "post_news_impulse_confirmed",
            "post_news_retest_level",
            "post_news_retest_status",
            "post_news_acceptance_status",
            "post_news_failed_move",
            "post_news_continuation_quality",
            "post_news_continuation_direction",
            "post_news_reasons",
            "post_news_blockers",
            "post_news_modifiers",
            "post_news_otd_model",
            "post_news_otd_candidate",
            "post_news_otd_direction",
            "post_news_otd_entry_model",
            "post_news_otd_first_impulse_chased",
            "post_news_otd_acceptance_confirmed",
            "post_news_otd_retest_confirmed",
            "post_news_otd_ltf_confirmed",
            "post_news_otd_real_target",
            "post_news_otd_stop_ok",
            "post_news_otd_practical_rr_ok",
            "post_news_otd_practical_rr",
            "post_news_otd_min_practical_rr",
            "post_news_otd_blockers",
            "post_news_otd_reasons",
        ):
            value = tracked_payload.get(key, tracked_metadata.get(key))
            if value is not None:
                raw_alert_payload[key] = value

        if TPO_SIGNAL_GATE_ENABLED and tpo_signal_permission in {"RESEARCH_ONLY", "BLOCK"}:
            raw_alert_payload["should_alert"] = False
            raw_alert_payload["telegram_allowed"] = False
            raw_alert_payload["telegram_block_reason"] = f"blocked_by_tpo:{tpo_signal_permission}:{tpo_signal_reason}"
            raw_alert_payload["signal_quality_reason"] = raw_alert_payload["telegram_block_reason"]
            return raw_alert_payload

        enriched_payload = enrich_payload_with_quality(raw_alert_payload)
        hard_allowed, hard_reason = _is_telegram_trade_alert_allowed(enriched_payload)
        enriched_payload["telegram_hard_gate_allowed"] = hard_allowed
        enriched_payload["telegram_hard_gate_reason"] = hard_reason

        if not hard_allowed:
            enriched_payload["should_alert"] = False
            enriched_payload["telegram_allowed"] = False
            enriched_payload["telegram_block_reason"] = hard_reason
            return enriched_payload

        if enriched_payload.get("telegram_allowed") is not True:
            enriched_payload["should_alert"] = False
            enriched_payload["telegram_block_reason"] = enriched_payload.get("signal_quality_reason")
            return enriched_payload

        fmt = format_signal_message(
            {
                "signal_id": enriched_payload.get("signal_id"),
                "symbol": symbol,
                "stage": signal_class,
                "signal_class": signal_class,
                "scenario": scenario,
                "direction": direction,
                "confidence": enriched_payload.get("confidence"),
                "probability": enriched_payload.get("confidence"),
                "market_state": market_state,
                "htf_bias": htf_bias,
                "rationale": enriched_payload.get("reason"),
                "reason": enriched_payload.get("reason"),
                "missing_conditions": enriched_payload.get("missing_conditions") or [],
                "execution_status": execution_status,
                "execution_model": enriched_payload.get("execution_model"),
                "entry_reference_price": entry_price,
                "invalidation_reference_price": stop_price,
                "target_reference_price": target_price,
                "risk_reward_ratio": rr,
                "practical_rr": rr,
                "execution_timeframe": enriched_payload.get("execution_timeframe"),
                "trigger_reason": enriched_payload.get("trigger_reason"),
                "open_behavior": enriched_payload.get("open_behavior"),
                "tpo_watch_state": enriched_payload.get("tpo_watch_state"),
                "ltf_model_state": enriched_payload.get("ltf_model_state"),
                "entry_model_hint": enriched_payload.get("entry_model_hint"),
                "stop_model_hint": enriched_payload.get("stop_model_hint"),
            }
        )

        enriched_payload["telegram_title"] = fmt.title
        enriched_payload["telegram_body"] = fmt.body
        enriched_payload["telegram_text"] = fmt.render()
        return enriched_payload

    def _find_setup_by_name(self, setups: list[Any], setup_name: Any) -> Any:
        setup_name = getattr(setup_name, "value", setup_name)
        if setup_name is None:
            return None

        for setup in setups:
            st = self._safe_attr(setup, "setup_type")
            st = getattr(st, "value", st)
            if st == setup_name:
                return setup
        return None

    def _extract_invalidation_level(self, setup: Any) -> Optional[float]:
        if setup is None:
            return None

        for attr_name in ["invalidation_level", "invalidated_by", "stop_level", "entry_invalidation"]:
            value = self._safe_attr(setup, attr_name)
            if value is not None:
                try:
                    return float(value)
                except Exception:
                    continue

        diagnostics = self._safe_attr(setup, "diagnostics")
        if diagnostics is not None:
            for attr_name in ["invalidation_level", "invalidated_by", "stop_level"]:
                value = self._safe_attr(diagnostics, attr_name)
                if value is not None:
                    try:
                        return float(value)
                    except Exception:
                        continue

        return None

    def _extract_target_zone(self, setup: Any) -> list[float]:
        if setup is None:
            return []

        candidates = [
            self._safe_attr(setup, "target_zone"),
            self._safe_attr(setup, "targets"),
            self._safe_attr(setup, "take_profit_zone"),
            self._safe_attr(setup, "tp_zone"),
        ]

        diagnostics = self._safe_attr(setup, "diagnostics")
        if diagnostics is not None:
            candidates.extend([
                self._safe_attr(diagnostics, "target_zone"),
                self._safe_attr(diagnostics, "targets"),
            ])

        for candidate in candidates:
            if candidate is None:
                continue
            if isinstance(candidate, (list, tuple)):
                out: list[float] = []
                for item in candidate:
                    try:
                        out.append(float(item))
                    except Exception:
                        pass
                if out:
                    return out
            else:
                try:
                    return [float(candidate)]
                except Exception:
                    pass

        return []

    def _extract_scenario_decision(
        self,
        context: Any,
        setups: list[Any],
        scenario: Any,
        final_signal: Any,
    ) -> str:
        if scenario is not None and not (
            isinstance(scenario, dict) and scenario.get("scenario_engine_failed")
        ):
            decision = self._safe_attr(scenario, "decision")
            return getattr(decision, "value", decision) or "NO_TRADE"

        alignment_score = infer_alignment_score(context, setups)
        final_status = self._safe_attr(final_signal, "status")
        final_status = getattr(final_status, "value", final_status)

        if final_status in {"READY", "ACTIVE", "WATCH"}:
            return "WATCH"
        if alignment_score is not None and alignment_score >= 0.35:
            return "WATCH"
        return "NO_TRADE"

    @staticmethod
    def _safe_attr(obj: Any, name: str) -> Any:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(name)
        return getattr(obj, name, None)

    @staticmethod
    def _instrument_sort_key(value: Any) -> str:
        if isinstance(value, Instrument):
            return value.value
        return str(value)

    @staticmethod
    def _normalize_batch_symbol(raw_symbol: Any) -> Instrument:
        if isinstance(raw_symbol, Instrument):
            return raw_symbol

        enum_value = getattr(raw_symbol, "value", None)
        enum_name = getattr(raw_symbol, "name", None)

        if enum_value is not None:
            try:
                return Instrument(enum_value)
            except Exception:
                pass

        if enum_name is not None and hasattr(Instrument, enum_name):
            return getattr(Instrument, enum_name)

        if isinstance(raw_symbol, str):
            try:
                return Instrument(raw_symbol)
            except Exception:
                pass

            if hasattr(Instrument, raw_symbol):
                return getattr(Instrument, raw_symbol)

            upper = raw_symbol.upper()
            if hasattr(Instrument, upper):
                return getattr(Instrument, upper)

        raise ValueError(f"Unsupported batch symbol: {raw_symbol!r}")


# =============================================================================
# MODULE-LEVEL ENTRYPOINT FOR CLOUD WORKER
# =============================================================================

def _build_loader_for_batch_group(batch_group: str) -> MarketDataLoader:
    """
    Provider routing layer.

    core / fx_major -> TwelveData
    indices         -> yfinance

    The returned MarketDataLoader exposes the same contract to the rest of the
    runner, so context/scenario/setup/statistics stay provider-agnostic.
    """
    normalized_group = str(batch_group or "core").strip().lower()
    cache = ParquetCache()

    if normalized_group == "indices":
        from app.providers.yfinance_provider_adapter import (
            YFinanceAdapterConfig,
            YFinanceProviderAdapter,
        )

        provider = YFinanceProviderAdapter(
            config=YFinanceAdapterConfig(
                debug=True,
                timeout_seconds=int(getattr(settings, "provider_timeout_sec", 30)),
            )
        )
        return MarketDataLoader(client=provider, cache=cache)

    if not settings.twelvedata_api_key:
        raise RuntimeError("TWELVEDATA_API_KEY is not configured in settings/environment.")

    td_client = TwelveDataClient(
        TwelveDataClientConfig(
            api_key=settings.twelvedata_api_key,
            debug=True,
            outputsize=500,
            timeout_seconds=int(getattr(settings, "provider_timeout_sec", 20)),
        )
    )

    provider = TwelveDataProviderAdapter(
        client=td_client,
        config=AdapterConfig(debug=True),
    )

    return MarketDataLoader(client=provider, cache=cache)


def run_batch_cycle(batch_group: str | None = None) -> dict[str, Any]:
    effective_batch_group = str(
        batch_group or getattr(settings, "batch_group", "core")
    ).strip().lower()

    loader = _build_loader_for_batch_group(effective_batch_group)

    runner = StatefulBatchRunner(
        loader=loader,
        minute_limit=12,
        batch_size=settings.default_batch_size,
        auto_mode=settings.auto_mode,
        force_batch=settings.force_batch,
        simulation_mode=settings.simulation_mode,
        batch_group=effective_batch_group,
    )
    return runner.run_batch_cycle()


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    result = run_batch_cycle()

    if result.get("instruments"):
        for inst in result["instruments"]:
            payload = _map_instrument_to_formatter_payload(inst)
            if payload is None:
                print("\n=== ALERT PAYLOAD DEBUG ===")
                print(f"Symbol: {inst.get('symbol')}")
                print(inst.get("alert_payload"))
                continue

            try:
                formatted = format_signal_message(payload)
                message_text = formatted.render()

                print("\n=== TELEGRAM MESSAGE ===")
                print(message_text)
                print("========================\n")

            except Exception as error:
                print(f"\n[TELEGRAM FORMAT ERROR] {inst.get('symbol')}: {error}")
                print(inst.get("alert_payload"))

    print(json.dumps(
        {
            "cycle_id": result.get("cycle_id"),
            "status": result.get("status"),
            "batch_group": result.get("meta", {}).get("batch_group"),
            "instrument_count": len(result.get("instruments", [])),
            "error_count": len(result.get("errors", [])),
            "skipped_count": result.get("meta", {}).get("skipped_count", 0),
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()