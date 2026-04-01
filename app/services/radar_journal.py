from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_JOURNAL_PATH = Path("runtime/radar_journal.ndjson")


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
            payload=payload or {},
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def append_event(
    event: JournalEvent,
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    path = Path(path)
    _ensure_parent_dir(path)

    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event.to_dict(), ensure_ascii=False))
        f.write("\n")


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
        payload=analysis_payload,
        path=path,
    )


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
        payload=signal_payload,
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
    _write_event(
        event_type="signal_registered",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        payload={
            "signal_id": signal_id,
            **payload,
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
    path: Path | str = DEFAULT_JOURNAL_PATH,
) -> None:
    _write_event(
        event_type="signal_updated",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        payload={
            "signal_id": signal_id,
            **payload,
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
    _write_event(
        event_type="signal_resolved",
        cycle_id=cycle_id,
        batch_id=batch_id,
        runner_version=runner_version,
        symbol=symbol,
        timeframe=timeframe,
        payload={
            "signal_id": signal_id,
            **payload,
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