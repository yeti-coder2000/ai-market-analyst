from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


KNOWN_INSTRUMENTS = {
    # core
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
    "BTCUSD",
    "ETHUSD",

    # fx_major
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "AUDUSD",

    # optional future fx reserve
    "NZDUSD",
    "EURJPY",
    "GBPJPY",
    "AUDJPY",

    # indices / commodities reserve
    "UKOIL",
    "GER40",
    "NAS100",
    "SPX500",

    # optional / reserve
    "DXY",
}


SYMBOL_ALIASES = {
    "GOLD": "XAUUSD",
    "XAU": "XAUUSD",
    "XAU/USD": "XAUUSD",
    "BTC/USD": "BTCUSD",
    "ETH/USD": "ETHUSD",
    "EUR/USD": "EURUSD",
    "GBP/USD": "GBPUSD",
    "USD/JPY": "USDJPY",
    "USD/CHF": "USDCHF",
    "USD/CAD": "USDCAD",
    "AUD/USD": "AUDUSD",
    "NZD/USD": "NZDUSD",
    "EUR/JPY": "EURJPY",
    "GBP/JPY": "GBPJPY",
    "AUD/JPY": "AUDJPY",
    "DAX": "GER40",
    "DE40": "GER40",
    "NDQ": "NAS100",
    "NDX": "NAS100",
    "NASDAQ": "NAS100",
    "SPX": "SPX500",
    "SP500": "SPX500",
    "SNP500": "SPX500",
    "S&P500": "SPX500",
    "BRENT": "UKOIL",
}


VALID_HTF_BIAS_VALUES = {"LONG", "SHORT", "NEUTRAL"}


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

DEFAULT_TTL_MINUTES_BY_STAGE = {
    "SCENARIO_FORMING": 180,
    "WATCH": 240,
    "READY": 180,
    "ACTIVE": 360,
}

SIGNAL_TRACKER_VERSION = "signal-tracker-v2.1-safety-context-pass-through"

SAFETY_CONTEXT_FIELDS = (
    "battle_permission",
    "telegram_delivery_mode",
    "battle_ready",
    "auction_context_score",
    "risk_mode",
    "scenario_family",
    "news_risk_state",
    "news_provider_status",
    "local_structure_damaged",
    "target_quality",
    "caution_flags",
)


@dataclass
class SignalTrackerResult:
    action: str  # NOOP | REGISTERED | UPDATED | RESOLVED
    signal_id: str | None
    payload: dict[str, Any]
    previous_payload: dict[str, Any] | None = None
    changed_fields: list[str] | None = None


class SignalTracker:
    """
    Signal lifecycle tracker v2.

    Responsibilities:
    - maintain runtime/open_signals.json
    - register / update / resolve open signals
    - expire stale signals via TTL
    - keep Telegram delivery metadata on signals
    - expose deterministic event-ready payloads for runner / journal / statistics
    """

    def __init__(
        self,
        open_signals_path: str | Path = "runtime/open_signals.json",
        *,
        ttl_minutes_by_stage: dict[str, int] | None = None,
    ) -> None:
        self.open_signals_path = Path(open_signals_path)
        self.open_signals_path.parent.mkdir(parents=True, exist_ok=True)
        self.ttl_minutes_by_stage = ttl_minutes_by_stage or deepcopy(DEFAULT_TTL_MINUTES_BY_STAGE)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(self, scenario_result: Any, cycle_id: str | None = None) -> SignalTrackerResult:
        """
        Main entrypoint.

        Behavior:
        - auto-expires stale signals before processing
        - NO_SETUP / NO_ACTION / NO_TRADE => no open signal action
        - WATCH / READY / ACTIVE => register or update open signal
        - RESOLVED => resolve existing signal
        """
        self.expire_stale_signals()

        payload = self._normalize_signal_payload(
            scenario_result=scenario_result,
            cycle_id=cycle_id,
        )

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
        if resolution not in RESOLUTION_TYPES:
            raise ValueError(f"Unsupported resolution: {resolution}")

        self.expire_stale_signals()

        payload = self._normalize_signal_payload(
            scenario_result=scenario_result,
            cycle_id=cycle_id,
        )
        payload["signal_class"] = "RESOLVED"
        payload["resolution"] = resolution
        payload["resolution_note"] = resolution_note

        return self._resolve_signal(payload)

    def expire_stale_signals(self) -> list[SignalTrackerResult]:
        """
        Resolve stale open signals as EXPIRED.
        """
        store = self._load_store()
        if not store:
            return []

        now = self._utc_now_dt()
        expired_results: list[SignalTrackerResult] = []
        ids_to_delete: list[str] = []

        for signal_id, payload in store.items():
            if payload.get("signal_class") not in OPEN_SIGNAL_STATES:
                continue

            if not self._is_signal_expired(payload, now=now):
                continue

            previous = deepcopy(payload)
            resolved = deepcopy(payload)
            resolved["signal_class"] = "RESOLVED"
            resolved["status"] = "RESOLVED"
            resolved["current_stage"] = "RESOLVED"
            resolved["resolution"] = "EXPIRED"
            resolved["resolution_note"] = "auto_expired_by_tracker_ttl"
            resolved["resolved_at_utc"] = now.isoformat()
            resolved["updated_at_utc"] = resolved["resolved_at_utc"]
            resolved["ttl_expires_at_utc"] = None

            changed_fields = self._diff_signal_fields(previous=previous, current=resolved)

            expired_results.append(
                SignalTrackerResult(
                    action="RESOLVED",
                    signal_id=signal_id,
                    payload=resolved,
                    previous_payload=previous,
                    changed_fields=changed_fields,
                )
            )
            ids_to_delete.append(signal_id)

        if ids_to_delete:
            for signal_id in ids_to_delete:
                store.pop(signal_id, None)
            self._save_store(store)

        return expired_results

    def load_open_signals(self) -> dict[str, dict[str, Any]]:
        return self._load_store()

    def mark_alert_sent(
        self,
        signal_id: str,
        *,
        alert_type: str | None = None,
        sent_at_utc: str | None = None,
    ) -> bool:
        if not signal_id:
            return False

        store = self._load_store()
        payload = store.get(signal_id)
        if payload is None:
            return False

        payload["was_sent_to_telegram"] = True
        payload["last_alert_type"] = alert_type
        payload["last_alerted_at_utc"] = sent_at_utc or self._utc_now()
        payload["updated_at_utc"] = self._utc_now()
        payload["state_version"] = int(payload.get("state_version", 1)) + 1

        store[signal_id] = payload
        self._save_store(store)
        return True

    def clear_alert_sent_flag(
        self,
        signal_id: str,
        *,
        keep_history: bool = True,
    ) -> bool:
        if not signal_id:
            return False

        store = self._load_store()
        payload = store.get(signal_id)
        if payload is None:
            return False

        payload["was_sent_to_telegram"] = False
        if not keep_history:
            payload["last_alert_type"] = None
            payload["last_alerted_at_utc"] = None

        payload["updated_at_utc"] = self._utc_now()
        payload["state_version"] = int(payload.get("state_version", 1)) + 1

        store[signal_id] = payload
        self._save_store(store)
        return True

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
            payload["state_version"] = 1

            payload["was_sent_to_telegram"] = False
            payload["last_alert_type"] = None
            payload["last_alerted_at_utc"] = None
            payload["was_deduped"] = False

            payload["current_stage"] = payload.get("signal_class")
            payload["ttl_expires_at_utc"] = self._compute_ttl_expires_at(payload)

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

        updated["was_sent_to_telegram"] = bool(previous.get("was_sent_to_telegram", False))
        updated["last_alert_type"] = previous.get("last_alert_type")
        updated["last_alerted_at_utc"] = previous.get("last_alerted_at_utc")
        updated["was_deduped"] = bool(previous.get("was_deduped", False))

        updated["current_stage"] = updated.get("signal_class")
        updated["ttl_expires_at_utc"] = self._compute_ttl_expires_at(updated)

        previous_stage = str(previous.get("signal_class") or "")
        new_stage = str(updated.get("signal_class") or "")
        previous_exec = str(previous.get("execution_status") or "")
        new_exec = str(updated.get("execution_status") or "")

        meaningful_stage_transition = (
            previous_stage != new_stage
            or (previous_exec != new_exec and new_exec == "EXECUTABLE")
        )

        if meaningful_stage_transition:
            updated["was_sent_to_telegram"] = False
            updated["last_alert_type"] = None

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
        updated["state_version"] = int(previous.get("state_version", 1)) + 1

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
        resolved["current_stage"] = "RESOLVED"
        resolved["resolution"] = payload.get("resolution")
        resolved["resolution_note"] = payload.get("resolution_note")
        resolved["resolved_at_utc"] = self._utc_now()
        resolved["updated_at_utc"] = resolved["resolved_at_utc"]
        resolved["ttl_expires_at_utc"] = None

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

        metadata = deepcopy(raw.get("metadata") or {})

        instrument = self._normalize_instrument_symbol(
            raw.get("instrument"),
            raw.get("symbol"),
            metadata.get("instrument"),
            metadata.get("symbol"),
        )
        scenario_type = self._extract_enum_value(raw.get("scenario_type")) or "NO_ACTION"
        direction = self._extract_enum_value(raw.get("direction")) or "NEUTRAL"
        market_state = self._extract_enum_value(raw.get("market_state")) or "TRANSITION"
        decision = self._extract_enum_value(raw.get("decision")) or "NO_TRADE"
        phase = self._extract_enum_value(raw.get("phase")) or "PRECONDITION"
        status = self._extract_enum_value(raw.get("status")) or "NO_SETUP"
        setup_type = self._extract_enum_value(raw.get("setup_type")) or "NONE"
        htf_bias = self._normalize_htf_bias(
            raw.get("htf_bias"),
            metadata.get("htf_bias"),
            self._nested_get(raw, "context", "htf_bias"),
            self._nested_get(raw, "behavioral_summary", "htf_bias"),
        )

        # Keep identity/context fields inside metadata too. This prevents downstream
        # journal/statistics exporters from losing symbol or HTF bias when the
        # scenario object itself does not expose them directly.
        metadata["symbol"] = instrument
        metadata["instrument"] = instrument
        if htf_bias:
            metadata["htf_bias"] = htf_bias

        # Defensive normalization: NO_ACTION must never carry directional trade state.
        if scenario_type == "NO_ACTION":
            direction = "NEUTRAL"
            decision = "NO_TRADE"
            status = "NO_SETUP"
            setup_type = "NONE"

        execution = self._normalize_execution(raw.get("execution"))
        signal_class = self._derive_signal_class(
            decision=decision,
            status=status,
            execution=execution,
        )

        # A non-READY signal must never be EXECUTABLE.
        if signal_class != "READY" and execution.get("status") == "EXECUTABLE":
            execution = self._force_not_executable(
                execution,
                reason="blocked_execution_before_ready",
            )

        trigger_reason = raw.get("trigger_reason")
        if trigger_reason is None:
            trigger_reason = execution.get("trigger_reason")

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
            "htf_bias": htf_bias,
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
            "reason": raw.get("reason") or raw.get("rationale"),
            "next_expected_event": raw.get("next_expected_event"),
            "missing_conditions": list(raw.get("missing_conditions") or []),
            "tags": list(raw.get("tags") or []),
            "metadata": metadata,
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
        payload["trigger_reason"] = trigger_reason

        signal_alignment = self._infer_signal_alignment(direction, htf_bias)
        payload["signal_alignment"] = signal_alignment
        payload["signal_alignment_marker"] = self._signal_alignment_marker(signal_alignment)
        payload["signal_alignment_label"] = self._signal_alignment_label(signal_alignment)

        stop_quality, stop_quality_reason, theoretical_rr, practical_rr = self._infer_stop_quality(payload)
        payload["stop_quality"] = stop_quality
        payload["stop_quality_reason"] = stop_quality_reason
        payload["theoretical_rr"] = theoretical_rr
        payload["practical_rr"] = practical_rr

        self._attach_safety_context_fields(
            payload=payload,
            raw=raw,
            metadata=metadata,
        )

        return payload

    def _attach_safety_context_fields(
        self,
        *,
        payload: dict[str, Any],
        raw: dict[str, Any],
        metadata: dict[str, Any],
    ) -> None:
        """
        Pass through Battle Gate / safety context fields when upstream stages
        already provide them.

        This tracker does not evaluate Battle Gate itself. It only preserves
        fields so journal/statistics/Telegram can see the same context if a
        later layer attached it before a tracker update.
        """
        for key in SAFETY_CONTEXT_FIELDS:
            value = self._first_non_empty_value(
                raw.get(key),
                metadata.get(key),
                self._nested_get(raw, "context", key),
                self._nested_get(raw, "metadata", key),
                self._nested_get(raw, "payload", key),
            )

            if value in (None, "", [], {}):
                continue

            payload[key] = value
            metadata[key] = value

        payload["metadata"] = metadata
        payload["signal_tracker_version"] = SIGNAL_TRACKER_VERSION


    def _normalize_execution(self, execution: Any) -> dict[str, Any]:
        raw = self._to_dict(execution)

        return {
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

        if status in {"IDLE", "EDGE_FORMING"}:
            return "SCENARIO_FORMING"

        if status == "WATCH":
            return "WATCH"

        if status == "READY":
            if execution_status == "EXECUTABLE":
                return "READY"
            return "WATCH"

        if status == "ACTIVE":
            return "ACTIVE"

        return "SCENARIO_FORMING"

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
        setup_type = payload.get("setup_type")
        trigger_reason = payload.get("trigger_reason")

        candidates: list[tuple[str, dict[str, Any]]] = []
        for signal_id, existing in store.items():
            if existing.get("signal_class") == "RESOLVED":
                continue
            if existing.get("symbol") != symbol:
                continue
            if existing.get("scenario") != scenario:
                continue
            if existing.get("direction") != direction:
                continue
            candidates.append((signal_id, existing))

        if not candidates:
            return None

        exact_matches: list[str] = []
        for signal_id, existing in candidates:
            same_setup = existing.get("setup_type") == setup_type
            same_trigger = existing.get("trigger_reason") == trigger_reason
            if same_setup and same_trigger:
                exact_matches.append(signal_id)

        if exact_matches:
            return self._pick_most_recent_signal_id(store, exact_matches)

        setup_matches = [
            signal_id
            for signal_id, existing in candidates
            if existing.get("setup_type") == setup_type
        ]
        if setup_matches:
            return self._pick_most_recent_signal_id(store, setup_matches)

        return self._pick_most_recent_signal_id(store, [sid for sid, _ in candidates])

    def _pick_most_recent_signal_id(
        self,
        store: dict[str, dict[str, Any]],
        signal_ids: list[str],
    ) -> str | None:
        if not signal_ids:
            return None

        def sort_key(signal_id: str) -> tuple[float, str]:
            payload = store.get(signal_id) or {}
            ts = payload.get("updated_at_utc") or payload.get("created_at_utc") or ""
            try:
                dt = datetime.fromisoformat(ts)
                return (dt.timestamp(), signal_id)
            except Exception:
                return (0.0, signal_id)

        signal_ids = sorted(signal_ids, key=sort_key, reverse=True)
        return signal_ids[0]

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
        merged["trigger_reason"] = merged.get("trigger_reason") or execution.get("trigger_reason")

        signal_alignment = self._infer_signal_alignment(merged.get("direction"), merged.get("htf_bias"))
        merged["signal_alignment"] = signal_alignment
        merged["signal_alignment_marker"] = self._signal_alignment_marker(signal_alignment)
        merged["signal_alignment_label"] = self._signal_alignment_label(signal_alignment)

        stop_quality, stop_quality_reason, theoretical_rr, practical_rr = self._infer_stop_quality(merged)
        merged["stop_quality"] = stop_quality
        merged["stop_quality_reason"] = stop_quality_reason
        merged["theoretical_rr"] = theoretical_rr
        merged["practical_rr"] = practical_rr

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
    # TTL helpers
    # ------------------------------------------------------------------

    def _compute_ttl_expires_at(self, payload: dict[str, Any]) -> str | None:
        stage = str(payload.get("signal_class") or "WATCH").upper()
        ttl_minutes = self.ttl_minutes_by_stage.get(stage)
        if ttl_minutes is None:
            return None

        now = self._utc_now_dt()
        return (now + timedelta(minutes=int(ttl_minutes))).isoformat()

    def _is_signal_expired(self, payload: dict[str, Any], *, now: datetime) -> bool:
        expires_at = payload.get("ttl_expires_at_utc")
        if not expires_at:
            return False

        try:
            expiry_dt = datetime.fromisoformat(expires_at)
        except Exception:
            return False

        return now >= expiry_dt

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _utc_now_dt() -> datetime:
        return datetime.now(timezone.utc)

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
    def _normalize_instrument_symbol(*values: Any) -> str:
        def iter_candidates(value: Any):
            if value is None:
                return

            enum_value = getattr(value, "value", None)
            if enum_value is not None:
                yield enum_value

            enum_name = getattr(value, "name", None)
            if enum_name is not None:
                yield enum_name

            if isinstance(value, dict):
                for key in ("symbol", "instrument", "ticker", "provider_symbol"):
                    if key in value:
                        yield from iter_candidates(value.get(key))
                metadata = value.get("metadata")
                if isinstance(metadata, dict):
                    yield from iter_candidates(metadata)
                return

            yield value

        for value in values:
            for candidate in iter_candidates(value):
                if candidate is None:
                    continue

                text = str(candidate).strip().upper()
                if not text or text == "UNKNOWN":
                    continue

                text = text.replace(" ", "")
                aliased = SYMBOL_ALIASES.get(text, text)
                compact = aliased.replace("/", "")

                if aliased in KNOWN_INSTRUMENTS:
                    return aliased

                if compact in KNOWN_INSTRUMENTS:
                    return compact

        return "UNKNOWN"

    @staticmethod
    def _normalize_htf_bias(*values: Any) -> str:
        for value in values:
            if value is None:
                continue

            enum_value = getattr(value, "value", None)
            if enum_value is not None:
                value = enum_value

            text = str(value).strip().upper()
            if text in VALID_HTF_BIAS_VALUES:
                return text

        return ""

    @staticmethod
    def _nested_get(obj: Any, *keys: str) -> Any:
        current = obj
        for key in keys:
            if current is None:
                return None
            if isinstance(current, dict):
                current = current.get(key)
            else:
                current = getattr(current, key, None)
        return current


    @staticmethod
    def _infer_signal_alignment(direction: Any, htf_bias: Any) -> str:
        direction_text = str(direction or "").strip().upper()
        htf_text = str(htf_bias or "").strip().upper()
        if direction_text not in {"LONG", "SHORT"}:
            return "NO_DIRECTION"
        if htf_text == "NEUTRAL" or not htf_text:
            return "NEUTRAL_HTF"
        if htf_text not in {"LONG", "SHORT"}:
            return "UNKNOWN_HTF"
        if direction_text == htf_text:
            return "TREND_ALIGNED"
        return "COUNTER_TREND"

    @staticmethod
    def _signal_alignment_marker(alignment: Any) -> str:
        mapping = {"TREND_ALIGNED":"🟢", "COUNTER_TREND":"🔴", "NEUTRAL_HTF":"⚪", "NO_DIRECTION":"⚫", "UNKNOWN_HTF":"⚫"}
        return mapping.get(str(alignment or "UNKNOWN_HTF").upper(), "⚫")

    @staticmethod
    def _signal_alignment_label(alignment: Any) -> str:
        mapping = {"TREND_ALIGNED":"TREND-ALIGNED", "COUNTER_TREND":"COUNTER-TREND", "NEUTRAL_HTF":"NEUTRAL HTF", "NO_DIRECTION":"NO DIRECTION", "UNKNOWN_HTF":"UNKNOWN HTF"}
        return mapping.get(str(alignment or "UNKNOWN_HTF").upper(), "UNKNOWN HTF")

    @staticmethod
    def _min_stop_distance_by_symbol(symbol: Any) -> float | None:
        mapping = {"XAUUSD":8.0, "BTCUSD":100.0, "ETHUSD":3.0, "EURUSD":0.00050, "GBPUSD":0.00060, "AUDUSD":0.00040, "USDCHF":0.00050, "USDCAD":0.00050, "USDJPY":0.08, "GER40":30.0, "NAS100":50.0, "SPX500":8.0, "UKOIL":0.20}
        return mapping.get(str(symbol or "").upper())

    def _infer_stop_quality(self, payload: dict[str, Any]) -> tuple[str, str | None, float | None, float | None]:
        symbol = payload.get("symbol") or payload.get("instrument")
        entry = self._float_or_none(payload.get("entry_reference_price"))
        stop = self._float_or_none(payload.get("invalidation_reference_price"))
        target = self._float_or_none(payload.get("target_reference_price"))
        rr = self._float_or_none(payload.get("risk_reward_ratio"))
        if entry is None or stop is None:
            return "UNKNOWN", None, rr, None
        min_stop = self._min_stop_distance_by_symbol(symbol)
        if min_stop is None:
            return "UNKNOWN", None, rr, rr
        stop_distance = abs(entry - stop)
        if stop_distance < min_stop:
            practical_rr = round(abs(target - entry) / min_stop, 3) if target is not None and min_stop else None
            return "TIGHT_STOP", f"stop_distance {stop_distance:.6f} below practical_min_stop {min_stop:.6f}", rr, practical_rr
        return "OK", None, rr, rr

    @staticmethod
    def _force_not_executable(execution: dict[str, Any], *, reason: str) -> dict[str, Any]:
        patched = deepcopy(execution or {})
        patched["status"] = "NOT_EXECUTABLE"
        patched["model"] = patched.get("model") or "NONE"
        patched["risk_reward_ratio"] = None
        patched["stop_distance"] = None
        patched["target_distance"] = None
        patched["execution_timeframe"] = None
        patched["trigger_reason"] = reason
        return patched

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

    @staticmethod
    def _first_non_empty_value(*values: Any) -> Any:
        for value in values:
            if value not in (None, "", [], {}):
                return value
        return None


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