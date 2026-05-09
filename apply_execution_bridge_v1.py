#!/usr/bin/env python3
"""
Apply Execution Bridge v1 to app/runners/stateful_batch_runner.py.

What this does:
- keeps Telegram gate unchanged;
- keeps provider routing / persistent runtime / batching unchanged;
- makes ScenarioEngine failures visible instead of silent ModuleNotFound fallback;
- enriches READY tracker payloads with a conservative execution plan;
- maps fallback scenario names to execution.py canonical names:
    RETURN_TO_VALUE_IN_PROGRESS / PRE_SWEEP_RETURN_* -> SWEEP_RETURN_LONG/SHORT
    PRE_CONTINUATION_* / IMPULSE_IN_PROGRESS       -> TREND_CONTINUATION_LONG/SHORT
- writes execution_bridge diagnostics into metadata.

Run from repo root:
    python apply_execution_bridge_v1.py
"""
from __future__ import annotations

import py_compile
import re
import shutil
from pathlib import Path

TARGET = Path("app/runners/stateful_batch_runner.py")
BACKUP = Path("app/runners/stateful_batch_runner.py.bak_execution_bridge_v1")


def fail(msg: str) -> None:
    raise SystemExit(f"[execution-bridge-v1] ERROR: {msg}")


def replace_once(text: str, pattern: str, replacement: str, *, flags: int = 0, label: str) -> str:
    new, count = re.subn(pattern, replacement, text, count=1, flags=flags)
    if count != 1:
        fail(f"replacement failed or matched {count} times: {label}")
    return new


def main() -> None:
    if not TARGET.exists():
        fail(f"target file not found: {TARGET}")

    original = TARGET.read_text(encoding="utf-8")
    text = original

    if not BACKUP.exists():
        shutil.copy2(TARGET, BACKUP)
        print(f"[execution-bridge-v1] backup created: {BACKUP}")
    else:
        print(f"[execution-bridge-v1] backup already exists: {BACKUP}")

    # ------------------------------------------------------------------
    # 1) Import execution builder
    # ------------------------------------------------------------------
    if "from app.scenarios.execution import build_execution_plan" not in text:
        text = replace_once(
            text,
            r"(from app\.scenarios\.behavioral import \([\s\S]*?\)\n)",
            r"\1from app.scenarios.execution import build_execution_plan\n",
            flags=re.MULTILINE,
            label="insert build_execution_plan import",
        )
        print("[execution-bridge-v1] import added")
    else:
        print("[execution-bridge-v1] import already present")

    # ------------------------------------------------------------------
    # 2) Runner version marker
    # ------------------------------------------------------------------
    text = re.sub(
        r'RUNNER_VERSION\s*=\s*"[^"]+"',
        'RUNNER_VERSION = "1.4.3-execution-bridge-v1"',
        text,
        count=1,
    )

    # ------------------------------------------------------------------
    # 3) Patch _run_scenario_engine diagnostics
    # ------------------------------------------------------------------
    new_run_scenario_engine = '''    def _run_scenario_engine(self, context: Any, setups: list[Any]) -> Any:
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

'''
    text = replace_once(
        text,
        r"    def _run_scenario_engine\(self, context: Any, setups: list\[Any\]\) -> Any:\n[\s\S]*?\n    def _select_final_signal",
        new_run_scenario_engine + "    def _select_final_signal",
        flags=re.MULTILINE,
        label="replace _run_scenario_engine",
    )
    print("[execution-bridge-v1] _run_scenario_engine patched")

    # ------------------------------------------------------------------
    # 4) Patch tracker source payload builder and add execution bridge helpers
    # ------------------------------------------------------------------
    new_tracker_block = '''    def _build_tracker_source_payload(
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
        - HTF bias is not NEUTRAL;
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
            return self._mark_execution_bridge_block(
                payload,
                reason="neutral_htf_bias",
                detail="HTF bias is NEUTRAL; execution bridge skipped",
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

'''
    text = replace_once(
        text,
        r"    def _build_tracker_source_payload\([\s\S]*?\n    def _force_payload_symbol",
        new_tracker_block + "    def _force_payload_symbol",
        flags=re.MULTILINE,
        label="replace _build_tracker_source_payload and insert bridge helpers",
    )
    print("[execution-bridge-v1] tracker payload builder + bridge helpers patched")

    # ------------------------------------------------------------------
    # 5) Patch call site in _analyze_symbol
    # ------------------------------------------------------------------
    old_call_pattern = r'''            tracker_source_payload = self\._build_tracker_source_payload\(
                source=scenario if scenario_ok else final_signal,
                symbol=symbol\.value,
                batch_group=self\.batch_group,
                cycle_id=cycle_id,
                price=price,
                market_state=market_state,
                htf_bias=htf_bias,
                phase=phase,
                scenario_type=scenario_type,
                scenario_probability=scenario_probability,
                scenario_decision=scenario_decision,
            \)

            tracker_result = self\.signal_tracker\.process\(
'''

    new_call = '''            scenario_engine_failed = isinstance(scenario, dict) and bool(scenario.get("scenario_engine_failed"))
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
            tracker_source_payload = self._enrich_tracker_payload_with_execution_bridge(
                payload=tracker_source_payload,
                context=context,
                scenario=scenario,
                final_signal=final_signal,
            )

            tracker_result = self.signal_tracker.process(
'''
    text = replace_once(
        text,
        old_call_pattern,
        new_call,
        flags=re.MULTILINE,
        label="patch tracker_source_payload call site",
    )
    print("[execution-bridge-v1] call site patched")

    TARGET.write_text(text, encoding="utf-8")

    try:
        py_compile.compile(str(TARGET), doraise=True)
    except Exception:
        TARGET.write_text(original, encoding="utf-8")
        print("[execution-bridge-v1] compile failed; original file restored")
        raise

    print("[execution-bridge-v1] compile OK")
    print("[execution-bridge-v1] done")


if __name__ == "__main__":
    main()
