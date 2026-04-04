from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
import json
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
from app.services.signal_tracker import SignalTracker, SignalTrackerResult
from app.services.statistics import build_and_export_statistics
from app.services.telegram_formatter import format_signal_message
from app.storage.cache_store import ParquetCache

logger = get_logger(__name__, component="stateful_batch_runner")

RUNNER_VERSION = "1.4.0"


# =============================================================================
# GUARANTEED HISTORY WRITE LAYER (PERSISTENT DISK READY)
# =============================================================================

import os

BASE_PATH = os.getenv("DATA_PATH", "/var/data")
RUNTIME_DIR = Path(BASE_PATH) / "runtime"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

JOURNAL_FALLBACK_PATH = RUNTIME_DIR / "radar_journal.ndjson"
SNAPSHOT_FALLBACK_PATH = RUNTIME_DIR / "radar_snapshot_v2.ndjson"


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

    # indices / oil
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


def _map_instrument_to_formatter_payload(inst: dict[str, Any]) -> Optional[dict[str, Any]]:
    alert_payload = inst.get("alert_payload")
    if not alert_payload:
        return None

    if not alert_payload.get("should_alert", False):
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
                    continue

                try:
                    instrument_result = self._analyze_symbol(
                        symbol,
                        cycle_id=cycle_id,
                        batch_id=batch_id,
                    )
                    normalized_instruments.append(instrument_result.to_dict())
                    processed_count += 1

                    if instrument_result.alert_payload and instrument_result.alert_payload.get("should_alert"):
                        alerts_count += 1

                    if instrument_result.status != "ok":
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
                    "alerts": alerts_count,
                    "duration_sec": round(elapsed_sec, 3),
                    "status": cycle_status,
                },
            )

            try:
                build_and_export_statistics()
            except Exception as stats_error:
                cycle_logger.warning(f"Statistics export failed: {stats_error}")

            cycle_logger.info(
                f"Batch cycle finished. batch_group={self.batch_group} status={cycle_status} instruments={len(normalized_instruments)} errors={len(cycle_errors)}"
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

            series_by_tf: dict[Timeframe, pd.DataFrame] = {}
            refreshed_timeframes: list[str] = []
            load_results: dict[str, Any] = {}

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

            if is_weekend_utc():
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

            tracker_result = self.signal_tracker.process(
                scenario_result=scenario if scenario_ok else final_signal,
                cycle_id=cycle_id,
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
                    signal_payload={**candidate_payload, "batch_group": self.batch_group},
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
        tracked_stage = tracked_payload.get("signal_class", "SCENARIO_FORMING")
        final_signal_normalized = self._normalize_final_signal(tracked_stage)
        watch_status = self._derive_watch_status_from_stage(tracked_stage)

        alert_payload = self._build_alert_payload_from_tracker_payload(
            symbol=symbol.value,
            tracked_payload=tracked_payload,
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

    def _run_analysis_pipeline(
        self,
        symbol: Instrument,
        series_by_tf: dict[Timeframe, pd.DataFrame],
    ) -> dict[str, Any]:
        context = self._build_context(symbol, series_by_tf)
        setups = self._run_setups(context)
        scenario = self._run_scenario_engine(context, setups)
        final_signal = self._select_final_signal(context, setups, scenario)

        return {
            "context": context,
            "setups": setups,
            "scenario": scenario,
            "final_signal": final_signal,
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

        except ModuleNotFoundError:
            return None

        except Exception as error:
            return {
                "scenario_engine_failed": True,
                "scenario_engine_error": str(error),
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
            },
            "meta": {
                "simulation_mode": self.simulation_mode,
                "batch_group": self.batch_group,
                "refreshed_timeframes": refreshed_timeframes,
                "data_source": "cache_only" if not refreshed_timeframes else "mixed_or_api",
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
            return "WATCH"
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

    def _build_alert_payload_from_tracker_payload(
        self,
        *,
        symbol: str,
        tracked_payload: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        stage = str(tracked_payload.get("signal_class") or "").upper()
        if stage not in {"WATCH", "READY", "ACTIVE"}:
            return None

        return {
            "should_alert": True,
            "signal_id": tracked_payload.get("signal_id"),
            "symbol": symbol,
            "batch_group": self.batch_group,
            "signal_class": tracked_payload.get("signal_class"),
            "stage": tracked_payload.get("signal_class"),
            "scenario": tracked_payload.get("scenario"),
            "direction": tracked_payload.get("direction"),
            "confidence": tracked_payload.get("confidence"),
            "entry_reference_price": tracked_payload.get("entry_reference_price"),
            "invalidation_reference_price": tracked_payload.get("invalidation_reference_price"),
            "target_reference_price": tracked_payload.get("target_reference_price"),
            "market_state": tracked_payload.get("market_state"),
            "htf_bias": tracked_payload.get("htf_bias"),
            "rationale": tracked_payload.get("rationale"),
            "reason": tracked_payload.get("next_expected_event"),
            "missing_conditions": tracked_payload.get("missing_conditions", []),
            "execution_status": tracked_payload.get("execution_status"),
            "execution_model": tracked_payload.get("execution_model"),
            "risk_reward_ratio": tracked_payload.get("risk_reward_ratio"),
            "execution_timeframe": tracked_payload.get("execution_timeframe"),
            "trigger_reason": tracked_payload.get("trigger_reason"),
        }

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

        # support enums from other modules (e.g. schema.Instrument)
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
            # try exact enum value
            try:
                return Instrument(raw_symbol)
            except Exception:
                pass

            # try enum name
            if hasattr(Instrument, raw_symbol):
                return getattr(Instrument, raw_symbol)

            upper = raw_symbol.upper()
            if hasattr(Instrument, upper):
                return getattr(Instrument, upper)

        raise ValueError(f"Unsupported batch symbol: {raw_symbol!r}")


# =============================================================================
# MODULE-LEVEL ENTRYPOINT FOR CLOUD WORKER
# =============================================================================

def run_batch_cycle(batch_group: str | None = None) -> dict[str, Any]:
    if not settings.twelvedata_api_key:
        raise RuntimeError("TWELVEDATA_API_KEY is not configured in settings/environment.")

    td_client = TwelveDataClient(
        TwelveDataClientConfig(
            api_key=settings.twelvedata_api_key,
            debug=True,
            outputsize=500,
            timeout_seconds=20,
        )
    )

    provider = TwelveDataProviderAdapter(
        client=td_client,
        config=AdapterConfig(debug=True),
    )

    cache = ParquetCache()
    loader = MarketDataLoader(client=provider, cache=cache)

    effective_batch_group = batch_group or getattr(settings, "batch_group", "core")

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
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()