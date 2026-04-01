from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


OPEN_SIGNAL_STATES = {
    "SCENARIO_FORMING",
    "WATCH",
    "READY",
    "ACTIVE",
}

RESOLVED_SIGNAL_STATES = {
    "RESOLVED",
}

RESOLUTION_TYPES = {
    "VALIDATED",
    "INVALIDATED",
    "EXPIRED",
}


@dataclass
class SignalTrackerResult:
    action: str  # NOOP | REGISTERED | UPDATED | RESOLVED
    signal_id: str | None
    payload: dict[str, Any]
    previous_payload: dict[str, Any] | None = None
    changed_fields: list[str] | None = None


class SignalTracker:
    """
    Production signal lifecycle tracker.

    Responsibilities:
    - maintain runtime/open_signals.json
    - register new open signals
    - update existing signals
    - resolve signals
    - expose event-ready payloads for radar_journal layer

    Expected input:
    - scenario_result (ScenarioResult or dict-like)
    """

    def __init__(self, open_signals_path: str | Path = "runtime/open_signals.json") -> None:
        self.open_signals_path = Path(open_signals_path)
        self.open_signals_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(self, scenario_result: Any, cycle_id: str | None = None) -> SignalTrackerResult:
        """
        Main entrypoint.

        Behavior:
        - NO_SETUP / NO_ACTION / NO_TRADE => no open signal action
        - WATCH / READY / ACTIVE => register or update open signal
        - RESOLVED => resolve existing signal
        """
        payload = self._normalize_signal_payload(scenario_result=scenario_result, cycle_id=cycle_id)

        signal_class = payload.get("signal_class")
        if signal_class in RESOLVED_SIGNAL_STATES:
            return self._resolve_signal(payload)

        if signal_class not in OPEN_SIGNAL_STATES:
            return SignalTrackerResult(
                action="NOOP",
                signal_id=None,
                payload=payload,
                previous_payload=None,
                changed_fields=[],
            )

        return self._register_or_update_signal(payload)

    def resolve(
        self,
        scenario_result: Any,
        resolution: str,
        cycle_id: str | None = None,
        resolution_note: str | None = None,
    ) -> SignalTrackerResult:
        """
        Explicit resolution API.

        Use this when downstream logic decides that an open signal is:
        - VALIDATED
        - INVALIDATED
        - EXPIRED
        """
        if resolution not in RESOLUTION_TYPES:
            raise ValueError(f"Unsupported resolution: {resolution}")

        payload = self._normalize_signal_payload(scenario_result=scenario_result, cycle_id=cycle_id)
        payload["signal_class"] = "RESOLVED"
        payload["resolution"] = resolution
        payload["resolution_note"] = resolution_note

        return self._resolve_signal(payload)

    def load_open_signals(self) -> dict[str, dict[str, Any]]:
        return self._load_store()

    # ------------------------------------------------------------------
    # Core lifecycle
    # ------------------------------------------------------------------

    def _register_or_update_signal(self, payload: dict[str, Any]) -> SignalTrackerResult:
        store = self._load_store()

        existing_id = self._find_matching_open_signal_id(store=store, payload=payload)

        if existing_id is None:
            signal_id = payload["signal_id"]
            now = self._utc_now()

            payload["created_at_utc"] = now
            payload["updated_at_utc"] = now
            payload["update_count"] = 0

            store[signal_id] = payload
            self._save_store(store)

            return SignalTrackerResult(
                action="REGISTERED",
                signal_id=signal_id,
                payload=payload,
                previous_payload=None,
                changed_fields=self._sorted_top_level_fields(payload),
            )

        previous = deepcopy(store[existing_id])
        updated = self._merge_signal_payload(existing=store[existing_id], incoming=payload)

        changed_fields = self._diff_signal_fields(previous=previous, current=updated)
        if not changed_fields:
            return SignalTrackerResult(
                action="NOOP",
                signal_id=existing_id,
                payload=updated,
                previous_payload=previous,
                changed_fields=[],
            )

        updated["updated_at_utc"] = self._utc_now()
        updated["update_count"] = int(previous.get("update_count", 0)) + 1

        store[existing_id] = updated
        self._save_store(store)

        return SignalTrackerResult(
            action="UPDATED",
            signal_id=existing_id,
            payload=updated,
            previous_payload=previous,
            changed_fields=changed_fields,
        )

    def _resolve_signal(self, payload: dict[str, Any]) -> SignalTrackerResult:
        store = self._load_store()

        existing_id = self._find_matching_open_signal_id(store=store, payload=payload)
        if existing_id is None:
            return SignalTrackerResult(
                action="NOOP",
                signal_id=None,
                payload=payload,
                previous_payload=None,
                changed_fields=[],
            )

        previous = deepcopy(store[existing_id])
        resolved = deepcopy(store[existing_id])

        resolved["signal_class"] = "RESOLVED"
        resolved["status"] = "RESOLVED"
        resolved["resolution"] = payload.get("resolution")
        resolved["resolution_note"] = payload.get("resolution_note")
        resolved["resolved_at_utc"] = self._utc_now()
        resolved["updated_at_utc"] = resolved["resolved_at_utc"]

        changed_fields = self._diff_signal_fields(previous=previous, current=resolved)

        del store[existing_id]
        self._save_store(store)

        return SignalTrackerResult(
            action="RESOLVED",
            signal_id=existing_id,
            payload=resolved,
            previous_payload=previous,
            changed_fields=changed_fields,
        )

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------

    def _normalize_signal_payload(
        self,
        scenario_result: Any,
        cycle_id: str | None,
    ) -> dict[str, Any]:
        raw = self._to_dict(scenario_result)

        instrument = self._extract_enum_value(raw.get("instrument")) or "UNKNOWN"
        scenario_type = self._extract_enum_value(raw.get("scenario_type")) or "NO_ACTION"
        direction = self._extract_enum_value(raw.get("direction")) or "NEUTRAL"
        market_state = self._extract_enum_value(raw.get("market_state")) or "TRANSITION"
        decision = self._extract_enum_value(raw.get("decision")) or "NO_TRADE"
        phase = self._extract_enum_value(raw.get("phase")) or "PRECONDITION"
        status = self._extract_enum_value(raw.get("status")) or "NO_SETUP"
        setup_type = self._extract_enum_value(raw.get("setup_type")) or "NONE"

        execution = self._normalize_execution(raw.get("execution"))
        signal_class = self._derive_signal_class(
            decision=decision,
            status=status,
            execution=execution,
        )

        payload = {
            "signal_id": self._build_signal_id(
                instrument=instrument,
                cycle_id=cycle_id,
                scenario_type=scenario_type,
                direction=direction,
            ),
            "symbol": instrument,
            "instrument": instrument,
            "cycle_id": cycle_id,
            "scenario": scenario_type,
            "scenario_type": scenario_type,
            "phase": phase,
            "decision": decision,
            "market_state": market_state,
            "direction": direction,
            "status": status,
            "signal_class": signal_class,
            "setup_type": setup_type,
            "setup_name": raw.get("setup_name"),
            "dominant_setup": raw.get("dominant_setup"),
            "price": self._float_or_none(raw.get("price")),
            "confidence": self._float_or_none(raw.get("confidence")),
            "alignment_score": self._float_or_none(raw.get("alignment_score")),
            "rationale": raw.get("rationale"),
            "next_expected_event": raw.get("next_expected_event"),
            "missing_conditions": list(raw.get("missing_conditions") or []),
            "tags": list(raw.get("tags") or []),
            "metadata": deepcopy(raw.get("metadata") or {}),
            "execution": execution,
        }

        payload["execution_status"] = execution.get("status")
        payload["execution_model"] = execution.get("model")
        payload["entry_reference_price"] = execution.get("entry_reference_price")
        payload["invalidation_reference_price"] = execution.get("invalidation_reference_price")
        payload["target_reference_price"] = execution.get("target_reference_price")
        payload["risk_reward_ratio"] = execution.get("risk_reward_ratio")
        payload["stop_distance"] = execution.get("stop_distance")
        payload["target_distance"] = execution.get("target_distance")
        payload["execution_timeframe"] = execution.get("execution_timeframe")
        payload["trigger_reason"] = execution.get("trigger_reason")

        return payload

    def _normalize_execution(self, execution: Any) -> dict[str, Any]:
        raw = self._to_dict(execution)

        normalized = {
            "status": raw.get("status", "NOT_EXECUTABLE"),
            "model": raw.get("model", "NONE"),
            "entry_reference_price": self._float_or_none(raw.get("entry_reference_price")),
            "invalidation_reference_price": self._float_or_none(raw.get("invalidation_reference_price")),
            "target_reference_price": self._float_or_none(raw.get("target_reference_price")),
            "risk_reward_ratio": self._float_or_none(raw.get("risk_reward_ratio")),
            "stop_distance": self._float_or_none(raw.get("stop_distance")),
            "target_distance": self._float_or_none(raw.get("target_distance")),
            "execution_timeframe": raw.get("execution_timeframe"),
            "trigger_reason": raw.get("trigger_reason"),
        }

        return normalized

    def _derive_signal_class(
        self,
        decision: str,
        status: str,
        execution: dict[str, Any],
    ) -> str:
        execution_status = execution.get("status")

        if status == "RESOLVED":
            return "RESOLVED"

        if decision == "NO_TRADE" or status in {"NO_SETUP"}:
            return "SCENARIO_FORMING"

        if status == "IDLE":
            return "SCENARIO_FORMING"

        if status == "WATCH":
            return "WATCH"

        if status == "READY":
            if execution_status == "EXECUTABLE":
                return "READY"
            return "WATCH"

        if status == "ACTIVE":
            return "ACTIVE"

        return "WATCH"

    # ------------------------------------------------------------------
    # Matching / merge / diff
    # ------------------------------------------------------------------

    def _find_matching_open_signal_id(
        self,
        store: dict[str, dict[str, Any]],
        payload: dict[str, Any],
    ) -> str | None:
        symbol = payload.get("symbol")
        scenario = payload.get("scenario")
        direction = payload.get("direction")

        for signal_id, existing in store.items():
            if existing.get("symbol") != symbol:
                continue
            if existing.get("scenario") != scenario:
                continue
            if existing.get("direction") != direction:
                continue
            if existing.get("signal_class") == "RESOLVED":
                continue
            return signal_id

        return None

    def _merge_signal_payload(
        self,
        existing: dict[str, Any],
        incoming: dict[str, Any],
    ) -> dict[str, Any]:
        merged = deepcopy(existing)

        for key, value in incoming.items():
            if key == "signal_id":
                continue

            if key == "metadata":
                merged["metadata"] = self._merge_dicts(
                    existing.get("metadata") or {},
                    value or {},
                )
                continue

            if key == "execution":
                merged["execution"] = self._merge_dicts(
                    existing.get("execution") or {},
                    value or {},
                )
                continue

            merged[key] = value

        # keep mirrored execution fields in sync
        execution = merged.get("execution") or {}
        merged["execution_status"] = execution.get("status")
        merged["execution_model"] = execution.get("model")
        merged["entry_reference_price"] = execution.get("entry_reference_price")
        merged["invalidation_reference_price"] = execution.get("invalidation_reference_price")
        merged["target_reference_price"] = execution.get("target_reference_price")
        merged["risk_reward_ratio"] = execution.get("risk_reward_ratio")
        merged["stop_distance"] = execution.get("stop_distance")
        merged["target_distance"] = execution.get("target_distance")
        merged["execution_timeframe"] = execution.get("execution_timeframe")
        merged["trigger_reason"] = execution.get("trigger_reason")

        return merged

    def _diff_signal_fields(
        self,
        previous: dict[str, Any],
        current: dict[str, Any],
    ) -> list[str]:
        changed: list[str] = []

        top_level_keys = sorted(set(previous.keys()) | set(current.keys()))
        for key in top_level_keys:
            if previous.get(key) != current.get(key):
                changed.append(key)

        return changed

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load_store(self) -> dict[str, dict[str, Any]]:
        if not self.open_signals_path.exists():
            return {}

        try:
            with self.open_signals_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

        if not isinstance(data, dict):
            return {}

        return data

    def _save_store(self, store: dict[str, dict[str, Any]]) -> None:
        with self.open_signals_path.open("w", encoding="utf-8") as f:
            json.dump(store, f, indent=2, ensure_ascii=False, sort_keys=True)

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _build_signal_id(
        instrument: str,
        cycle_id: str | None,
        scenario_type: str,
        direction: str,
    ) -> str:
        safe_cycle_id = cycle_id or datetime.now(timezone.utc).isoformat()
        safe_cycle_id = safe_cycle_id.replace(":", "-")
        return f"{instrument}_{safe_cycle_id}_{scenario_type}_{direction}"

    @staticmethod
    def _extract_enum_value(value: Any) -> Any:
        return getattr(value, "value", value)

    @staticmethod
    def _float_or_none(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _merge_dicts(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        result = deepcopy(base)
        for key, value in incoming.items():
            result[key] = value
        return result

    @staticmethod
    def _sorted_top_level_fields(payload: dict[str, Any]) -> list[str]:
        return sorted(payload.keys())

    def _to_dict(self, obj: Any) -> dict[str, Any]:
        if obj is None:
            return {}

        if isinstance(obj, dict):
            return deepcopy(obj)

        if hasattr(obj, "model_dump"):
            return obj.model_dump()

        if hasattr(obj, "dict"):
            return obj.dict()

        if hasattr(obj, "__dict__"):
            return deepcopy(obj.__dict__)

        return {}