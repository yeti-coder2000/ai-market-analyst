from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from app.core.logger import get_logger
from app.core.settings import settings


logger = get_logger(__name__, component="alert_deduper")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _parse_iso(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _normalize_float(value: Any) -> str:
    if value is None:
        return ""
    try:
        return f"{float(value):.4f}"
    except Exception:
        return str(value)


def _normalize_zone(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "|".join(_normalize_float(v) for v in value)
    return _normalize_float(value)


@dataclass(frozen=True)
class AlertFingerprint:
    symbol: str
    alert_type: str
    scenario_type: str
    direction: str
    invalidation_level: str
    target_zone: str

    def to_key(self) -> str:
        raw = "|".join(
            [
                self.symbol.upper(),
                self.alert_type.upper(),
                self.scenario_type.lower(),
                self.direction.upper(),
                self.invalidation_level,
                self.target_zone,
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class AlertDeduper:
    """
    Prevents repeated Telegram alerts for the same market state.

    Strategy:
    - build a stable fingerprint from alert payload
    - persist sent alerts to alerts_state.json
    - suppress duplicates within cooldown window
    - allow new alert when structure meaningfully changes
    """

    def __init__(
        self,
        state_path: Optional[Path] = None,
        cooldown_sec: Optional[int] = None,
    ) -> None:
        self.state_path = state_path or settings.alerts_state_path
        self.cooldown_sec = cooldown_sec if cooldown_sec is not None else settings.alert_cooldown_sec
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._load_state()

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {
                "updated_at": _utc_now_iso(),
                "alerts": {},
            }

        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("alerts_state root is not a dict")
            alerts = raw.get("alerts", {})
            if not isinstance(alerts, dict):
                alerts = {}
            return {
                "updated_at": raw.get("updated_at", _utc_now_iso()),
                "alerts": alerts,
            }
        except Exception:
            logger.exception(
                "Failed to load alerts state. Reinitializing deduper state.",
                extra={"component": "alert_deduper", "cycle_id": "-", "symbol": "-"},
            )
            return {
                "updated_at": _utc_now_iso(),
                "alerts": {},
            }

    def _save_state(self) -> None:
        self._state["updated_at"] = _utc_now_iso()
        tmp_path = self.state_path.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self.state_path)

    def _build_fingerprint(self, payload: dict[str, Any]) -> AlertFingerprint:
        return AlertFingerprint(
            symbol=str(payload.get("symbol", "")).strip().upper(),
            alert_type=str(payload.get("alert_type", "")).strip().upper(),
            scenario_type=str(payload.get("scenario_type", "")).strip().lower(),
            direction=str(payload.get("direction", "")).strip().upper(),
            invalidation_level=_normalize_float(payload.get("invalidation_level")),
            target_zone=_normalize_zone(payload.get("target_zone")),
        )

    def _symbol_bucket(self, symbol: str) -> dict[str, Any]:
        alerts = self._state.setdefault("alerts", {})
        bucket = alerts.setdefault(symbol.upper(), {})
        return bucket

    def should_send(self, payload: dict[str, Any]) -> tuple[bool, str]:
        """
        Returns:
            (should_send, reason)

        Reasons:
        - new_alert
        - cooldown_expired
        - changed_structure
        - duplicate_within_cooldown
        - invalid_payload
        """
        symbol = str(payload.get("symbol", "")).strip().upper()
        alert_type = str(payload.get("alert_type", "")).strip().upper()

        if not symbol or not alert_type:
            return False, "invalid_payload"

        fingerprint = self._build_fingerprint(payload)
        fingerprint_key = fingerprint.to_key()

        bucket = self._symbol_bucket(symbol)
        previous = bucket.get(alert_type)

        if not previous:
            return True, "new_alert"

        previous_key = str(previous.get("fingerprint_key", ""))
        last_sent_at = _parse_iso(previous.get("last_sent_at"))
        cooldown_deadline = (
            last_sent_at + timedelta(seconds=self.cooldown_sec) if last_sent_at else None
        )
        now = _utc_now()

        if previous_key != fingerprint_key:
            return True, "changed_structure"

        if cooldown_deadline and now >= cooldown_deadline:
            return True, "cooldown_expired"

        return False, "duplicate_within_cooldown"

    def mark_sent(self, payload: dict[str, Any], reason: str = "sent") -> None:
        symbol = str(payload.get("symbol", "")).strip().upper()
        alert_type = str(payload.get("alert_type", "")).strip().upper()

        if not symbol or not alert_type:
            logger.warning(
                "Skipping mark_sent due to invalid payload.",
                extra={"component": "alert_deduper", "cycle_id": payload.get("cycle_id", "-"), "symbol": symbol or "-"},
            )
            return

        fingerprint = self._build_fingerprint(payload)
        bucket = self._symbol_bucket(symbol)

        bucket[alert_type] = {
            "fingerprint_key": fingerprint.to_key(),
            "fingerprint": {
                "symbol": fingerprint.symbol,
                "alert_type": fingerprint.alert_type,
                "scenario_type": fingerprint.scenario_type,
                "direction": fingerprint.direction,
                "invalidation_level": fingerprint.invalidation_level,
                "target_zone": fingerprint.target_zone,
            },
            "last_sent_at": _utc_now_iso(),
            "reason": reason,
            "cycle_id": str(payload.get("cycle_id", "")),
        }

        self._save_state()

        logger.info(
            f"Alert marked as sent. reason={reason}",
            extra={
                "component": "alert_deduper",
                "cycle_id": str(payload.get("cycle_id", "-")),
                "symbol": symbol,
            },
        )

    def prune(self, older_than_days: int = 14) -> int:
        """
        Removes stale alert history to keep state file clean.
        Returns number of removed entries.
        """
        cutoff = _utc_now() - timedelta(days=older_than_days)
        removed = 0

        alerts = self._state.get("alerts", {})
        for symbol, symbol_bucket in list(alerts.items()):
            if not isinstance(symbol_bucket, dict):
                continue

            for alert_type, alert_record in list(symbol_bucket.items()):
                sent_at = _parse_iso(alert_record.get("last_sent_at"))
                if sent_at is None or sent_at < cutoff:
                    symbol_bucket.pop(alert_type, None)
                    removed += 1

            if not symbol_bucket:
                alerts.pop(symbol, None)

        if removed:
            self._save_state()
            logger.info(
                f"Pruned stale alert records: removed={removed}",
                extra={"component": "alert_deduper", "cycle_id": "-", "symbol": "-"},
            )

        return removed

    def snapshot(self) -> dict[str, Any]:
        return self._state