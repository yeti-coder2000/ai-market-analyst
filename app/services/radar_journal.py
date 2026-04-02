from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_JOURNAL_PATH = Path("runtime/radar_journal.ndjson")
SNAPSHOT_JOURNAL_PATH = Path("runtime/radar_snapshot_v2.ndjson")


@dataclass
class JournalEvent:
    event_id: str
    event_type: str
    ts_utc: str
    cycle_id: str
    batch_id: str
    runner_version: str
    symbol: str
    timeframe: str
    source: str
    status: str
    payload: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        event_type: str,
        cycle_id: str,
        batch_id: str,
        runner_version: str,
        symbol: str = "-",
        timeframe: str = "15m",
        source: str = "stateful_batch_runner",
        status: str = "ok",
        payload: Optional[Dict[str, Any]] = None,
    ) -> "JournalEvent":
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            ts_utc=datetime.now(timezone.utc).isoformat(),
            cycle_id=cycle_id,
            batch_id=batch_id,
            runner_version=runner_version,
            symbol=symbol,
            timeframe=timeframe,
            source=source,
            status=status,
            payload=_normalize_for_json(payload or {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        return _normalize_for_json(asdict(self))


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def append_event(
    event: JournalEvent,
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    path = Path(path)
    _ensure_parent_dir(path)

    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(event.to_dict(), ensure_ascii=False))
        f.write("\n")
        f.flush()


def append_snapshot(
    record: Dict[str, Any],
    path: Path | str = SNAPSHOT_JOURNAL_PATH,
) -> None:
    path = Path(path)
    _ensure_parent_dir(path)

    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(_normalize_for_json(record), ensure_ascii=False))
        f.write("\n")
        f.flush()


def _write_event(
    *,
    event_type: str,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str = "-",
    timeframe: str = "15m",
    source: str = "stateful_batch_runner",
    status: str = "ok",
    payload: Optional[Dict[str, Any]] = None,
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    event = JournalEvent.create(
        event_type=event_type,
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        source=source,
        status=status,
        payload=payload,
    )
    append_event(event, path=path)


def write_cycle_started(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    instruments: list[str],
    batch_size: int,
    auto_mode: bool,
    simulation_mode: bool,
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    _write_event(
        event_type="cycle_started",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        payload={
            "instruments": instruments,
            "batch_size": batch_size,
            "auto_mode": auto_mode,
            "simulation_mode": simulation_mode,
        },
        path=path,
    )


def write_instrument_analyzed(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str,
    timeframe: str,
    analysis_payload: Dict[str, Any],
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    _write_event(
        event_type="instrument_analyzed",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        payload=_compact_analysis_payload(analysis_payload),
        path=path,
    )


def write_instrument_snapshot(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str,
    timeframe: str,
    analysis_payload: Dict[str, Any],
    path: Path | str = SNAPSHOT_JOURNAL_PATH,
) -> None:
    record = _build_snapshot_record(
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        analysis_payload=analysis_payload,
    )
    append_snapshot(record, path=path)


def write_signal_candidate_detected(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str,
    timeframe: str,
    signal_payload: Dict[str, Any],
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    _write_event(
        event_type="signal_candidate_detected",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        payload=_compact_signal_payload(signal_payload),
        path=path,
    )


def write_signal_registered(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str,
    timeframe: str,
    signal_id: str,
    payload: Dict[str, Any],
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    signal_payload = _compact_signal_payload(payload)
    _write_event(
        event_type="signal_registered",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        payload={
            "signal_id": signal_id,
            **signal_payload,
        },
        path=path,
    )


def write_signal_updated(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str,
    timeframe: str,
    signal_id: str,
    payload: Dict[str, Any],
    previous_payload: Optional[Dict[str, Any]] = None,
    changed_fields: Optional[list[str]] = None,
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    current_payload = _compact_signal_payload(payload)
    previous_compact = _compact_signal_payload(previous_payload or {}) if previous_payload else None

    _write_event(
        event_type="signal_updated",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        payload={
            "signal_id": signal_id,
            "changed_fields": changed_fields or [],
            "execution_changed": _has_execution_change(changed_fields or []),
            "payload": current_payload,
            "previous_payload": previous_compact,
        },
        path=path,
    )


def write_signal_resolved(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str,
    timeframe: str,
    signal_id: str,
    payload: Dict[str, Any],
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    signal_payload = _compact_signal_payload(payload)
    _write_event(
        event_type="signal_resolved",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        payload={
            "signal_id": signal_id,
            **signal_payload,
        },
        path=path,
    )


def write_signal_deduped(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str,
    timeframe: str,
    dedupe_key: str,
    reason: str,
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    _write_event(
        event_type="signal_deduped",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        payload={
            "dedupe_key": dedupe_key,
            "reason": reason,
        },
        path=path,
    )


def write_telegram_sent(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str,
    timeframe: str,
    signal_id: str,
    message_type: str,
    chat_name: str,
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    _write_event(
        event_type="telegram_sent",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        payload={
            "signal_id": signal_id,
            "message_type": message_type,
            "chat_name": chat_name,
        },
        path=path,
    )


def write_telegram_failed(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str,
    timeframe: str,
    signal_id: str,
    error: str,
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    _write_event(
        event_type="telegram_failed",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        status="error",
        payload={
            "signal_id": signal_id,
            "error": error,
        },
        path=path,
    )


def write_cycle_finished(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    processed: int,
    errors: int,
    alerts: int,
    duration_sec: float,
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    _write_event(
        event_type="cycle_finished",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        status="ok" if errors == 0 else "error",
        payload={
            "processed": processed,
            "errors": errors,
            "alerts": alerts,
            "duration_sec": round(duration_sec, 3),
        },
        path=path,
    )


# ---------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------


def _build_snapshot_record(
    *,
    cycle_id: str,
    batch_id: str,
    runner_version: str,
    symbol: str,
    timeframe: str,
    analysis_payload: Dict[str, Any],
) -> Dict[str, Any]:
    raw = _normalize_for_json(analysis_payload)

    consistency_block = raw.get("consistency") or {
        "is_consistent": raw.get("consistency_ok"),
        "consistency_score": raw.get("consistency_score"),
        "conflict_flags": raw.get("conflict_flags") or [],
        "warnings": raw.get("consistency_warnings") or [],
        "summary": raw.get("consistency_summary"),
    }

    return {
        "schema_version": "2.0",
        "ts": datetime.now(timezone.utc).isoformat(),
        "cycle_id": cycle_id,
        "batch_id": batch_id,
        "runner_version": runner_version,
        "instrument": raw.get("instrument") or raw.get("symbol") or symbol,
        "timeframe": timeframe,
        "price": raw.get("price"),
        "market_state": raw.get("market_state"),
        "htf_bias": raw.get("htf_bias"),
        "phase": raw.get("phase"),
        "context": raw.get("context") or {},
        "setups": raw.get("setups") or {},
        "scenario": raw.get("scenario") or {},
        "final_signal": raw.get("final_signal") or {},
        "behavioral_summary": raw.get("behavioral_summary") or {},
        "consistency": consistency_block or {},
        "meta": raw.get("meta") or {},
    }


# ---------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------


def _normalize_for_json(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    if is_dataclass(value):
        return _normalize_for_json(asdict(value))

    if hasattr(value, "model_dump"):
        return _normalize_for_json(value.model_dump())

    if hasattr(value, "dict"):
        return _normalize_for_json(value.dict())

    if isinstance(value, dict):
        return {str(k): _normalize_for_json(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_normalize_for_json(v) for v in value]

    if hasattr(value, "__dict__"):
        return _normalize_for_json(vars(value))

    return str(value)


def _compact_analysis_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = _normalize_for_json(payload)

    return {
        "symbol": raw.get("symbol") or raw.get("instrument"),
        "price": raw.get("price"),
        "market_state": raw.get("market_state"),
        "direction": raw.get("direction"),
        "status": raw.get("status"),
        "setup_type": raw.get("setup_type"),
        "confidence": raw.get("confidence"),
        "alignment_score": raw.get("alignment_score"),
        "scenario": raw.get("scenario") or raw.get("scenario_type"),
        "phase": raw.get("phase"),
        "htf_bias": raw.get("htf_bias"),
        "final_signal": raw.get("final_signal"),
        "behavioral_summary": raw.get("behavioral_summary"),
        "consistency": raw.get("consistency"),
    }


def _compact_signal_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = _normalize_for_json(payload)
    execution = raw.get("execution") or {}

    compact = {
        "symbol": raw.get("symbol") or raw.get("instrument"),
        "scenario": raw.get("scenario") or raw.get("scenario_type"),
        "phase": raw.get("phase"),
        "decision": raw.get("decision"),
        "market_state": raw.get("market_state"),
        "direction": raw.get("direction"),
        "status": raw.get("status"),
        "signal_class": raw.get("signal_class"),
        "setup_type": raw.get("setup_type"),
        "setup_name": raw.get("setup_name"),
        "dominant_setup": raw.get("dominant_setup"),
        "price": raw.get("price"),
        "confidence": raw.get("confidence"),
        "alignment_score": raw.get("alignment_score"),
        "rationale": raw.get("rationale"),
        "next_expected_event": raw.get("next_expected_event"),
        "missing_conditions": raw.get("missing_conditions") or [],
        "tags": raw.get("tags") or [],
        "execution": {
            "status": execution.get("status"),
            "model": execution.get("model"),
            "entry_reference_price": execution.get("entry_reference_price"),
            "invalidation_reference_price": execution.get("invalidation_reference_price"),
            "target_reference_price": execution.get("target_reference_price"),
            "risk_reward_ratio": execution.get("risk_reward_ratio"),
            "stop_distance": execution.get("stop_distance"),
            "target_distance": execution.get("target_distance"),
            "execution_timeframe": execution.get("execution_timeframe"),
            "trigger_reason": execution.get("trigger_reason"),
        },
        "execution_status": raw.get("execution_status") or execution.get("status"),
        "execution_model": raw.get("execution_model") or execution.get("model"),
        "entry_reference_price": raw.get("entry_reference_price") or execution.get("entry_reference_price"),
        "invalidation_reference_price": raw.get("invalidation_reference_price") or execution.get("invalidation_reference_price"),
        "target_reference_price": raw.get("target_reference_price") or execution.get("target_reference_price"),
        "risk_reward_ratio": raw.get("risk_reward_ratio") or execution.get("risk_reward_ratio"),
        "trigger_reason": raw.get("trigger_reason") or execution.get("trigger_reason"),
        "metadata": raw.get("metadata") or {},
    }

    return compact


def _has_execution_change(changed_fields: list[str]) -> bool:
    execution_related = {
        "execution",
        "execution_status",
        "execution_model",
        "entry_reference_price",
        "invalidation_reference_price",
        "target_reference_price",
        "risk_reward_ratio",
        "stop_distance",
        "target_distance",
        "execution_timeframe",
        "trigger_reason",
    }
    return any(field in execution_related for field in changed_fields)