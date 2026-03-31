from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from app.core.logger import get_logger
from app.core.settings import settings


logger = get_logger(__name__, component="heartbeat")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class HeartbeatState:
    service: str
    status: str
    started_at: str
    last_started_at: str
    last_success_at: str
    last_error_at: str
    last_error_message: str
    last_cycle_id: str
    consecutive_failures: int
    total_cycles: int
    total_success_cycles: int
    total_failed_cycles: int
    updated_at: str

    @classmethod
    def fresh(cls, service: str) -> "HeartbeatState":
        now = _utc_now_iso()
        return cls(
            service=service,
            status="booting",
            started_at=now,
            last_started_at="",
            last_success_at="",
            last_error_at="",
            last_error_message="",
            last_cycle_id="",
            consecutive_failures=0,
            total_cycles=0,
            total_success_cycles=0,
            total_failed_cycles=0,
            updated_at=now,
        )


class HeartbeatService:
    """
    Lightweight persistent health tracker for the cloud worker.

    Responsibilities:
    - track worker boot
    - track cycle start / success / failure
    - persist latest health state to JSON
    - survive restarts by reloading previous state when possible
    """

    def __init__(self, heartbeat_path: Optional[Path] = None, service_name: Optional[str] = None) -> None:
        self.heartbeat_path = heartbeat_path or settings.heartbeat_path
        self.service_name = service_name or settings.app_name
        self.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._load_or_init()

    @property
    def state(self) -> HeartbeatState:
        return self._state

    def _load_or_init(self) -> HeartbeatState:
        if not self.heartbeat_path.exists():
            state = HeartbeatState.fresh(service=self.service_name)
            self._write(state)
            return state

        try:
            raw = json.loads(self.heartbeat_path.read_text(encoding="utf-8"))
            state = HeartbeatState(
                service=raw.get("service", self.service_name),
                status=raw.get("status", "unknown"),
                started_at=raw.get("started_at", _utc_now_iso()),
                last_started_at=raw.get("last_started_at", ""),
                last_success_at=raw.get("last_success_at", ""),
                last_error_at=raw.get("last_error_at", ""),
                last_error_message=raw.get("last_error_message", ""),
                last_cycle_id=raw.get("last_cycle_id", ""),
                consecutive_failures=int(raw.get("consecutive_failures", 0)),
                total_cycles=int(raw.get("total_cycles", 0)),
                total_success_cycles=int(raw.get("total_success_cycles", 0)),
                total_failed_cycles=int(raw.get("total_failed_cycles", 0)),
                updated_at=raw.get("updated_at", _utc_now_iso()),
            )
            return state
        except Exception:
            logger.exception(
                "Failed to load heartbeat file. Reinitializing new state.",
                extra={"component": "heartbeat", "cycle_id": "-", "symbol": "-"},
            )
            state = HeartbeatState.fresh(service=self.service_name)
            self._write(state)
            return state

    def _write(self, state: HeartbeatState) -> None:
        payload = asdict(state)
        tmp_path = self.heartbeat_path.with_suffix(".tmp")

        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self.heartbeat_path)

    def _commit(self) -> None:
        self._state.updated_at = _utc_now_iso()
        self._write(self._state)

    def mark_boot(self) -> None:
        now = _utc_now_iso()
        self._state.service = self.service_name
        self._state.status = "booting"
        self._state.started_at = now
        self._state.last_started_at = now
        self._state.last_error_message = ""
        self._commit()

        logger.info(
            "Heartbeat boot marked.",
            extra={"component": "heartbeat", "cycle_id": "-", "symbol": "-"},
        )

    def mark_idle(self) -> None:
        self._state.status = "idle"
        self._commit()

    def mark_cycle_started(self, cycle_id: str) -> None:
        self._state.status = "running"
        self._state.last_cycle_id = cycle_id
        self._state.last_started_at = _utc_now_iso()
        self._state.total_cycles += 1
        self._commit()

        logger.info(
            "Heartbeat cycle started.",
            extra={"component": "heartbeat", "cycle_id": cycle_id, "symbol": "-"},
        )

    def mark_cycle_success(self, cycle_id: str) -> None:
        self._state.status = "healthy"
        self._state.last_cycle_id = cycle_id
        self._state.last_success_at = _utc_now_iso()
        self._state.last_error_message = ""
        self._state.consecutive_failures = 0
        self._state.total_success_cycles += 1
        self._commit()

        logger.info(
            "Heartbeat cycle success.",
            extra={"component": "heartbeat", "cycle_id": cycle_id, "symbol": "-"},
        )

    def mark_cycle_failure(self, cycle_id: str, error_message: str) -> None:
        self._state.status = "degraded"
        self._state.last_cycle_id = cycle_id
        self._state.last_error_at = _utc_now_iso()
        self._state.last_error_message = error_message[:2000]
        self._state.consecutive_failures += 1
        self._state.total_failed_cycles += 1
        self._commit()

        logger.error(
            f"Heartbeat cycle failure: {error_message}",
            extra={"component": "heartbeat", "cycle_id": cycle_id, "symbol": "-"},
        )

    def mark_stopped(self) -> None:
        self._state.status = "stopped"
        self._commit()

        logger.info(
            "Heartbeat stopped.",
            extra={"component": "heartbeat", "cycle_id": "-", "symbol": "-"},
        )

    def snapshot(self) -> dict[str, Any]:
        return asdict(self._state)