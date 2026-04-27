from __future__ import annotations

from typing import Any

from app.context.schema import (
    Direction,
    MarketState,
    SetupStatus,
    SetupType,
)
from app.scenarios.execution import ExecutionPlan, build_execution_plan
from app.scenarios.schema import (
    ScenarioDecision,
    ScenarioEvidence,
    ScenarioPhase,
    ScenarioResult,
    ScenarioType,
)


MIN_READY_RR = 2.0
MAX_READY_RR = 10.0


class ScenarioEngine:
    """
    Rule-based scenario resolver.

    v1.2 state-machine guard:
    - EDGE_FORMING is reconnaissance only.
    - WATCH is surveillance only.
    - READY is the only state that may receive EXECUTABLE execution.
    - READY is downgraded to WATCH if execution geometry/RR is not good enough.
    - SCENARIO_FORMING + EXECUTABLE is globally blocked.
    - NO_ACTION is always NEUTRAL and NOT_EXECUTABLE.
    """

    def run(self, context: Any, setups: list[Any]) -> ScenarioResult:
        setup_a = setups[0] if len(setups) > 0 else None
        setup_b = setups[1] if len(setups) > 1 else None

        evidence = self._build_evidence(context=context, setup_a=setup_a, setup_b=setup_b)

        trend_result = self._resolve_trend_continuation(
            context=context,
            setup_a=setup_a,
            evidence=evidence,
        )
        if trend_result is not None:
            return trend_result

        sweep_result = self._resolve_sweep_return(
            context=context,
            setup_b=setup_b,
            evidence=evidence,
        )
        if sweep_result is not None:
            return sweep_result

        return self._build_no_action_result(context=context, evidence=evidence)

    # ------------------------------------------------------------------
    # Scenario resolvers
    # ------------------------------------------------------------------

    def _resolve_trend_continuation(
        self,
        context: Any,
        setup_a: Any,
        evidence: ScenarioEvidence,
    ) -> ScenarioResult | None:
        market_state = evidence.market_state
        htf_bias = evidence.htf_bias

        if market_state != MarketState.TREND.value:
            return None

        if htf_bias not in {Direction.LONG.value, Direction.SHORT.value}:
            return None

        direction = Direction(htf_bias)
        scenario_type = (
            ScenarioType.TREND_CONTINUATION_LONG
            if direction == Direction.LONG
            else ScenarioType.TREND_CONTINUATION_SHORT
        )

        setup_a_status = evidence.setup_a_status

        # READY: only this branch may attempt real execution.
        if setup_a_status == SetupStatus.READY.value:
            execution = build_execution_plan(
                context=context,
                scenario_type=scenario_type,
                direction=direction,
                evidence=evidence,
            )
            execution_payload = self._guard_execution_payload(
                execution=self._to_execution_payload(execution),
                allow_executable=True,
                setup_status=SetupStatus.READY.value,
                scenario_type=scenario_type.value,
                direction=direction.value,
            )
            decision, status, quality_reason = self._resolve_ready_tradeability(execution_payload)

            return ScenarioResult(
                instrument=self._context_instrument(context),
                price=self._context_price(context),
                scenario_type=scenario_type,
                phase=ScenarioPhase.CONFIRMED if status == SetupStatus.READY else ScenarioPhase.TRIGGER_ZONE,
                decision=decision,
                market_state=getattr(context, "market_state"),
                direction=direction,
                status=status,
                setup_type=SetupType.IMPULSE_PULLBACK_CONTINUATION,
                dominant_setup="setup_a",
                setup_name=SetupType.IMPULSE_PULLBACK_CONTINUATION.value,
                rationale=(
                    "Trend continuation scenario confirmed by setup engine. "
                    if status == SetupStatus.READY
                    else "Trend continuation setup is structurally READY, but execution quality is not sufficient. "
                )
                + quality_reason,
                confidence=self._clamp_confidence(
                    base=0.72 if status == SetupStatus.READY else 0.58,
                    setup_conf=evidence.setup_a_confidence,
                    bonus=0.10 if evidence.pullback_held_structure else 0.0,
                ),
                next_expected_event=(
                    "continuation_trigger"
                    if execution_payload.get("status") == "EXECUTABLE"
                    else "better_execution_geometry"
                ),
                missing_conditions=(
                    []
                    if execution_payload.get("status") == "EXECUTABLE"
                    else [quality_reason]
                ),
                alignment_score=self._infer_alignment_score(context, [setup_a]),
                tags=["trend", "continuation", direction.value.lower(), "ready_guarded"],
                evidence=evidence,
                execution=execution_payload,
                metadata={
                    "engine_version": "scenario_engine_v1_2",
                    "resolver": "trend_continuation",
                    "maturity": "ready" if status == SetupStatus.READY else "ready_blocked",
                    "execution_status": execution_payload.get("status"),
                    "execution_quality_reason": quality_reason,
                },
            )

        # WATCH: impulse exists, but pullback is not confirmed.
        if evidence.impulse_detected and not evidence.pullback_detected:
            execution_payload = self._guard_execution_payload(
                execution=self._to_execution_payload(
                    self._not_executable_plan(
                        model="LIMIT_ON_RETEST",
                        trigger_reason="waiting_for_pullback_confirmation",
                    )
                ),
                allow_executable=False,
                setup_status=SetupStatus.WATCH.value,
                scenario_type=scenario_type.value,
                direction=direction.value,
            )

            return ScenarioResult(
                instrument=self._context_instrument(context),
                price=self._context_price(context),
                scenario_type=scenario_type,
                phase=ScenarioPhase.TRIGGER_ZONE,
                decision=ScenarioDecision.WATCH,
                market_state=getattr(context, "market_state"),
                direction=direction,
                status=SetupStatus.WATCH,
                setup_type=SetupType.IMPULSE_PULLBACK_CONTINUATION,
                dominant_setup="setup_a",
                setup_name=SetupType.IMPULSE_PULLBACK_CONTINUATION.value,
                rationale=(
                    "Trend continuation developing: impulse exists in trend context, "
                    "but pullback confirmation is still missing."
                ),
                confidence=self._clamp_confidence(
                    base=0.52,
                    setup_conf=evidence.setup_a_confidence,
                    bonus=0.05,
                ),
                next_expected_event="pullback_confirmation",
                missing_conditions=["pullback"],
                alignment_score=self._infer_alignment_score(context, [setup_a]),
                tags=["trend", "impulse", "watch", direction.value.lower()],
                evidence=evidence,
                execution=execution_payload,
                metadata={
                    "engine_version": "scenario_engine_v1_2",
                    "resolver": "trend_continuation",
                    "maturity": "watch",
                    "execution_status": execution_payload.get("status"),
                },
            )

        # EDGE_FORMING: trend context exists, no trigger yet.
        missing = self._collect_missing_conditions([setup_a]) or ["impulse", "pullback"]
        execution_payload = self._guard_execution_payload(
            execution=self._to_execution_payload(
                self._not_executable_plan(
                    model="NONE",
                    trigger_reason="edge_forming_waiting_for_impulse",
                )
            ),
            allow_executable=False,
            setup_status=SetupStatus.EDGE_FORMING.value,
            scenario_type=scenario_type.value,
            direction=direction.value,
        )

        return ScenarioResult(
            instrument=self._context_instrument(context),
            price=self._context_price(context),
            scenario_type=scenario_type,
            phase=ScenarioPhase.PRECONDITION,
            decision=ScenarioDecision.WATCH,
            market_state=getattr(context, "market_state"),
            direction=direction,
            status=SetupStatus.EDGE_FORMING,
            setup_type=SetupType.IMPULSE_PULLBACK_CONTINUATION,
            dominant_setup="setup_a",
            setup_name=SetupType.IMPULSE_PULLBACK_CONTINUATION.value,
            rationale=(
                "EDGE_FORMING: trend context and HTF alignment are present, "
                "but continuation structure is not ready yet."
            ),
            confidence=self._clamp_confidence(
                base=0.35,
                setup_conf=evidence.setup_a_confidence,
                bonus=0.05,
            ),
            next_expected_event=(
                "bullish_impulse" if direction == Direction.LONG else "bearish_impulse"
            ),
            missing_conditions=missing,
            alignment_score=self._infer_alignment_score(context, [setup_a]),
            tags=["trend", "precondition", "edge_forming", direction.value.lower()],
            evidence=evidence,
            execution=execution_payload,
            metadata={
                "engine_version": "scenario_engine_v1_2",
                "resolver": "trend_continuation",
                "maturity": "edge_forming",
                "execution_status": execution_payload.get("status"),
            },
        )

    def _resolve_sweep_return(
        self,
        context: Any,
        setup_b: Any,
        evidence: ScenarioEvidence,
    ) -> ScenarioResult | None:
        market_state = evidence.market_state

        if market_state not in {MarketState.BALANCE.value, MarketState.TRANSITION.value}:
            return None

        setup_b_status = evidence.setup_b_status

        # EDGE_FORMING: no sweep yet.
        if not evidence.sweep_detected:
            direction = self._infer_sweep_watch_direction(evidence.htf_bias)
            if direction == Direction.NEUTRAL:
                return None

            scenario_type = (
                ScenarioType.SWEEP_RETURN_LONG
                if direction == Direction.LONG
                else ScenarioType.SWEEP_RETURN_SHORT
            )

            execution_payload = self._guard_execution_payload(
                execution=self._to_execution_payload(
                    self._not_executable_plan(
                        model="NONE",
                        trigger_reason="edge_forming_waiting_for_liquidity_sweep",
                    )
                ),
                allow_executable=False,
                setup_status=SetupStatus.EDGE_FORMING.value,
                scenario_type=scenario_type.value,
                direction=direction.value,
            )

            missing = self._collect_missing_conditions([setup_b]) or ["sweep", "return_to_value"]

            return ScenarioResult(
                instrument=self._context_instrument(context),
                price=self._context_price(context),
                scenario_type=scenario_type,
                phase=ScenarioPhase.PRECONDITION,
                decision=ScenarioDecision.WATCH,
                market_state=getattr(context, "market_state"),
                direction=direction,
                status=SetupStatus.EDGE_FORMING,
                setup_type=SetupType.SWEEP_RETURN_TO_VALUE,
                dominant_setup="setup_b",
                setup_name=SetupType.SWEEP_RETURN_TO_VALUE.value,
                rationale=(
                    "EDGE_FORMING: balance/transition context supports sweep-return scenario; "
                    "waiting for liquidity sweep."
                ),
                confidence=self._clamp_confidence(
                    base=0.22,
                    setup_conf=evidence.setup_b_confidence,
                    bonus=0.03,
                ),
                next_expected_event=(
                    "liquidity_sweep_low" if direction == Direction.LONG else "liquidity_sweep_high"
                ),
                missing_conditions=missing,
                alignment_score=self._infer_alignment_score(context, [setup_b]),
                tags=["sweep", "precondition", "edge_forming", direction.value.lower()],
                evidence=evidence,
                execution=execution_payload,
                metadata={
                    "engine_version": "scenario_engine_v1_2",
                    "resolver": "sweep_return",
                    "maturity": "edge_forming",
                    "execution_status": execution_payload.get("status"),
                },
            )

        if evidence.sweep_direction not in {Direction.LONG.value, Direction.SHORT.value}:
            return None

        direction = Direction(evidence.sweep_direction)
        scenario_type = (
            ScenarioType.SWEEP_RETURN_LONG
            if direction == Direction.LONG
            else ScenarioType.SWEEP_RETURN_SHORT
        )

        # READY: sweep + return-to-value confirmed. Only this branch may execute.
        if setup_b_status == SetupStatus.READY.value and evidence.return_to_value:
            execution = build_execution_plan(
                context=context,
                scenario_type=scenario_type,
                direction=direction,
                evidence=evidence,
            )
            execution_payload = self._guard_execution_payload(
                execution=self._to_execution_payload(execution),
                allow_executable=True,
                setup_status=SetupStatus.READY.value,
                scenario_type=scenario_type.value,
                direction=direction.value,
            )
            decision, status, quality_reason = self._resolve_ready_tradeability(execution_payload)

            return ScenarioResult(
                instrument=self._context_instrument(context),
                price=self._context_price(context),
                scenario_type=scenario_type,
                phase=ScenarioPhase.CONFIRMED if status == SetupStatus.READY else ScenarioPhase.TRIGGER_ZONE,
                decision=decision,
                market_state=getattr(context, "market_state"),
                direction=direction,
                status=status,
                setup_type=SetupType.SWEEP_RETURN_TO_VALUE,
                dominant_setup="setup_b",
                setup_name=SetupType.SWEEP_RETURN_TO_VALUE.value,
                rationale=(
                    "Sweep-return scenario confirmed by setup engine. "
                    if status == SetupStatus.READY
                    else "Sweep-return setup is structurally READY, but execution quality is not sufficient. "
                )
                + quality_reason,
                confidence=self._clamp_confidence(
                    base=0.70 if status == SetupStatus.READY else 0.58,
                    setup_conf=evidence.setup_b_confidence,
                    bonus=0.08,
                ),
                next_expected_event=(
                    "entry_trigger"
                    if execution_payload.get("status") == "EXECUTABLE"
                    else "better_execution_geometry"
                ),
                missing_conditions=(
                    []
                    if execution_payload.get("status") == "EXECUTABLE"
                    else [quality_reason]
                ),
                alignment_score=self._infer_alignment_score(context, [setup_b]),
                tags=["sweep", "return_to_value", direction.value.lower(), "ready_guarded"],
                evidence=evidence,
                execution=execution_payload,
                metadata={
                    "engine_version": "scenario_engine_v1_2",
                    "resolver": "sweep_return",
                    "maturity": "ready" if status == SetupStatus.READY else "ready_blocked",
                    "execution_status": execution_payload.get("status"),
                    "execution_quality_reason": quality_reason,
                },
            )

        # WATCH: sweep detected, waiting for return-to-value.
        if evidence.sweep_detected and not evidence.return_to_value:
            execution_payload = self._guard_execution_payload(
                execution=self._to_execution_payload(
                    self._not_executable_plan(
                        model="LIMIT_ON_RETEST",
                        trigger_reason="waiting_for_return_to_value_confirmation",
                    )
                ),
                allow_executable=False,
                setup_status=SetupStatus.WATCH.value,
                scenario_type=scenario_type.value,
                direction=direction.value,
            )

            return ScenarioResult(
                instrument=self._context_instrument(context),
                price=self._context_price(context),
                scenario_type=scenario_type,
                phase=ScenarioPhase.TRIGGER_ZONE,
                decision=ScenarioDecision.WATCH,
                market_state=getattr(context, "market_state"),
                direction=direction,
                status=SetupStatus.WATCH,
                setup_type=SetupType.SWEEP_RETURN_TO_VALUE,
                dominant_setup="setup_b",
                setup_name=SetupType.SWEEP_RETURN_TO_VALUE.value,
                rationale="Sweep detected, but price has not yet clearly returned to value.",
                confidence=self._clamp_confidence(
                    base=0.50,
                    setup_conf=evidence.setup_b_confidence,
                    bonus=0.04,
                ),
                next_expected_event="return_to_value_confirmation",
                missing_conditions=["return_to_value"],
                alignment_score=self._infer_alignment_score(context, [setup_b]),
                tags=["sweep", "watch", direction.value.lower()],
                evidence=evidence,
                execution=execution_payload,
                metadata={
                    "engine_version": "scenario_engine_v1_2",
                    "resolver": "sweep_return",
                    "maturity": "watch",
                    "execution_status": execution_payload.get("status"),
                },
            )

        # EDGE_FORMING: sweep exists but return context is not mature.
        missing = self._collect_missing_conditions([setup_b])
        if "return_to_value" not in missing:
            missing.append("return_to_value")

        execution_payload = self._guard_execution_payload(
            execution=self._to_execution_payload(
                self._not_executable_plan(
                    model="NONE",
                    trigger_reason="edge_forming_waiting_for_return_to_value",
                )
            ),
            allow_executable=False,
            setup_status=SetupStatus.EDGE_FORMING.value,
            scenario_type=scenario_type.value,
            direction=direction.value,
        )

        return ScenarioResult(
            instrument=self._context_instrument(context),
            price=self._context_price(context),
            scenario_type=scenario_type,
            phase=ScenarioPhase.PRECONDITION,
            decision=ScenarioDecision.WATCH,
            market_state=getattr(context, "market_state"),
            direction=direction,
            status=SetupStatus.EDGE_FORMING,
            setup_type=SetupType.SWEEP_RETURN_TO_VALUE,
            dominant_setup="setup_b",
            setup_name=SetupType.SWEEP_RETURN_TO_VALUE.value,
            rationale=(
                "EDGE_FORMING: sweep context exists, but return-to-value setup is not fully built yet."
            ),
            confidence=self._clamp_confidence(
                base=0.34,
                setup_conf=evidence.setup_b_confidence,
                bonus=0.05,
            ),
            next_expected_event="return_to_value_confirmation",
            missing_conditions=missing,
            alignment_score=self._infer_alignment_score(context, [setup_b]),
            tags=["sweep", "precondition", "edge_forming", direction.value.lower()],
            evidence=evidence,
            execution=execution_payload,
            metadata={
                "engine_version": "scenario_engine_v1_2",
                "resolver": "sweep_return",
                "maturity": "edge_forming",
                "execution_status": execution_payload.get("status"),
            },
        )

    def _build_no_action_result(
        self,
        context: Any,
        evidence: ScenarioEvidence,
    ) -> ScenarioResult:
        execution_payload = self._guard_execution_payload(
            execution=self._to_execution_payload(
                self._not_executable_plan(
                    model="NONE",
                    trigger_reason="no_dominant_scenario",
                )
            ),
            allow_executable=False,
            setup_status=SetupStatus.NO_SETUP.value,
            scenario_type=ScenarioType.NO_ACTION.value,
            direction=Direction.NEUTRAL.value,
        )

        return ScenarioResult(
            instrument=self._context_instrument(context),
            price=self._context_price(context),
            scenario_type=ScenarioType.NO_ACTION,
            phase=ScenarioPhase.PRECONDITION,
            decision=ScenarioDecision.NO_TRADE,
            market_state=getattr(context, "market_state"),
            direction=Direction.NEUTRAL,
            status=SetupStatus.NO_SETUP,
            setup_type=SetupType.NONE,
            dominant_setup=None,
            setup_name=None,
            rationale="No dominant scenario is currently confirmed.",
            confidence=0.10,
            next_expected_event=self._infer_next_expected_event(context),
            missing_conditions=self._collect_missing_conditions([]),
            alignment_score=self._infer_alignment_score(context, []),
            tags=["no_action"],
            evidence=evidence,
            execution=execution_payload,
            metadata={
                "engine_version": "scenario_engine_v1_2",
                "resolver": "fallback",
                "maturity": "none",
                "execution_status": execution_payload.get("status"),
            },
        )

    # ------------------------------------------------------------------
    # Global execution guard / tradeability
    # ------------------------------------------------------------------

    def _guard_execution_payload(
        self,
        *,
        execution: dict[str, Any],
        allow_executable: bool,
        setup_status: str,
        scenario_type: str,
        direction: str,
    ) -> dict[str, Any]:
        guarded = dict(execution or {})
        guarded.setdefault("status", "NOT_EXECUTABLE")
        guarded.setdefault("model", "NONE")

        if scenario_type == ScenarioType.NO_ACTION.value or direction == Direction.NEUTRAL.value:
            return self._force_not_executable(
                guarded,
                reason="blocked_no_action_or_neutral_direction",
            )

        if setup_status != SetupStatus.READY.value or not allow_executable:
            return self._force_not_executable(
                guarded,
                reason="blocked_pre_ready_execution",
            )

        if guarded.get("status") != "EXECUTABLE":
            return guarded

        rr = self._float_or_none(guarded.get("risk_reward_ratio"))
        if rr is None:
            return self._force_not_executable(guarded, reason="blocked_missing_rr")
        if rr < MIN_READY_RR:
            return self._force_not_executable(guarded, reason="blocked_rr_below_ready_threshold")
        if rr > MAX_READY_RR:
            return self._force_not_executable(guarded, reason="blocked_rr_above_sane_threshold")

        return guarded

    @staticmethod
    def _force_not_executable(execution: dict[str, Any], *, reason: str) -> dict[str, Any]:
        updated = dict(execution or {})
        updated["status"] = "NOT_EXECUTABLE"
        updated["model"] = updated.get("model") or "NONE"
        updated["risk_reward_ratio"] = None
        updated["stop_distance"] = None
        updated["target_distance"] = None
        updated["trigger_reason"] = reason
        return updated

    def _resolve_ready_tradeability(
        self,
        execution_payload: dict[str, Any],
    ) -> tuple[ScenarioDecision, SetupStatus, str]:
        status = execution_payload.get("status")
        reason = str(execution_payload.get("trigger_reason") or "execution_checked")

        if status == "EXECUTABLE":
            return ScenarioDecision.TRADEABLE, SetupStatus.READY, "execution_quality_ok"

        return ScenarioDecision.WATCH, SetupStatus.WATCH, reason

    # ------------------------------------------------------------------
    # Evidence / scoring / helpers
    # ------------------------------------------------------------------

    def _build_evidence(
        self,
        context: Any,
        setup_a: Any,
        setup_b: Any,
    ) -> ScenarioEvidence:
        impulse = getattr(context, "impulse", None)
        pullback = getattr(context, "pullback", None)
        sweep = getattr(context, "sweep", None)
        acceptance = getattr(context, "acceptance", None)

        setup_a_diag = getattr(setup_a, "diagnostics", None)
        setup_b_diag = getattr(setup_b, "diagnostics", None)

        passed: list[str] = []
        failed: list[str] = []

        passed.extend(self._condition_names(getattr(setup_a_diag, "passed_conditions", [])))
        passed.extend(self._condition_names(getattr(setup_b_diag, "passed_conditions", [])))
        failed.extend(self._condition_names(getattr(setup_a_diag, "failed_conditions", [])))
        failed.extend(self._condition_names(getattr(setup_b_diag, "failed_conditions", [])))

        notes = list(getattr(context, "notes", []) or [])

        return ScenarioEvidence(
            market_state=self._enum_value(getattr(context, "market_state", None)),
            htf_bias=self._enum_value(getattr(getattr(context, "htf_bias", None), "bias", None)),
            impulse_detected=bool(getattr(impulse, "detected", False)),
            impulse_direction=self._enum_value(getattr(impulse, "direction", None)),
            pullback_detected=bool(getattr(pullback, "detected", False)),
            pullback_direction=self._enum_value(getattr(pullback, "direction", None)),
            pullback_held_structure=bool(getattr(pullback, "held_structure", False)),
            sweep_detected=bool(getattr(sweep, "detected", False)),
            sweep_direction=self._enum_value(getattr(sweep, "direction", None)),
            return_to_value=bool(getattr(sweep, "returned_to_value", False)),
            acceptance_above=bool(getattr(acceptance, "accepted_above", False)),
            acceptance_below=bool(getattr(acceptance, "accepted_below", False)),
            no_acceptance_above=bool(getattr(acceptance, "no_acceptance_above", False)),
            no_acceptance_below=bool(getattr(acceptance, "no_acceptance_below", False)),
            setup_a_status=self._enum_value(getattr(setup_a, "status", None)),
            setup_a_direction=self._enum_value(getattr(setup_a, "direction", None)),
            setup_a_confidence=self._float_or_none(getattr(setup_a, "confidence", None)),
            setup_b_status=self._enum_value(getattr(setup_b, "status", None)),
            setup_b_direction=self._enum_value(getattr(setup_b, "direction", None)),
            setup_b_confidence=self._float_or_none(getattr(setup_b, "confidence", None)),
            passed_conditions=self._dedupe_keep_order(passed),
            failed_conditions=self._dedupe_keep_order(failed),
            notes=notes,
        )

    def _infer_next_expected_event(self, context: Any) -> str | None:
        market_state = self._enum_value(getattr(context, "market_state", None))
        htf_bias = self._enum_value(getattr(getattr(context, "htf_bias", None), "bias", None))

        impulse = getattr(context, "impulse", None)
        pullback = getattr(context, "pullback", None)
        sweep = getattr(context, "sweep", None)

        impulse_detected = bool(getattr(impulse, "detected", False))
        pullback_detected = bool(getattr(pullback, "detected", False))
        sweep_detected = bool(getattr(sweep, "detected", False))
        returned_to_value = bool(getattr(sweep, "returned_to_value", False))

        if sweep_detected and not returned_to_value:
            return "return_to_value_confirmation"

        if impulse_detected and not pullback_detected:
            return "pullback_confirmation"

        if market_state == MarketState.TREND.value:
            if htf_bias == Direction.LONG.value:
                return "bullish_impulse"
            if htf_bias == Direction.SHORT.value:
                return "bearish_impulse"

        if market_state in {MarketState.BALANCE.value, MarketState.TRANSITION.value}:
            if htf_bias == Direction.LONG.value:
                return "liquidity_sweep_low"
            if htf_bias == Direction.SHORT.value:
                return "liquidity_sweep_high"

        return "structure_confirmation"

    def _infer_alignment_score(self, context: Any, setups: list[Any]) -> float:
        score = 0.0

        market_state = self._enum_value(getattr(context, "market_state", None))
        htf_bias = self._enum_value(getattr(getattr(context, "htf_bias", None), "bias", None))

        if market_state in {
            MarketState.TREND.value,
            MarketState.TRANSITION.value,
            MarketState.BALANCE.value,
        }:
            score += 0.15

        if htf_bias in {Direction.LONG.value, Direction.SHORT.value}:
            score += 0.25

        impulse = getattr(context, "impulse", None)
        pullback = getattr(context, "pullback", None)
        sweep = getattr(context, "sweep", None)

        if bool(getattr(impulse, "detected", False)):
            score += 0.15

        if bool(getattr(pullback, "detected", False)):
            score += 0.15

        if bool(getattr(sweep, "detected", False)):
            score += 0.15

        if bool(getattr(sweep, "returned_to_value", False)):
            score += 0.05

        for setup in setups:
            if setup is None:
                continue
            status = self._enum_value(getattr(setup, "status", None))
            if status == SetupStatus.EDGE_FORMING.value:
                score += 0.03
            elif status == SetupStatus.WATCH.value:
                score += 0.05
            elif status == SetupStatus.READY.value:
                score += 0.10

        return round(min(score, 1.0), 2)

    def _collect_missing_conditions(self, setups: list[Any]) -> list[str]:
        missing: list[str] = []

        for setup in setups:
            if setup is None:
                continue

            diagnostics = getattr(setup, "diagnostics", None)
            failed = getattr(diagnostics, "failed_conditions", []) or []

            for item in failed:
                name = getattr(item, "name", None)
                if name is not None and str(name) not in missing:
                    missing.append(str(name))

        return missing

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _not_executable_plan(
        *,
        model: str = "NONE",
        trigger_reason: str | None = None,
    ) -> ExecutionPlan:
        return ExecutionPlan(
            status="NOT_EXECUTABLE",
            model=model,
            execution_timeframe=None,
            trigger_reason=trigger_reason,
        )

    @staticmethod
    def _to_execution_payload(execution: Any) -> dict[str, Any]:
        if execution is None:
            return {"status": "NOT_EXECUTABLE", "model": "NONE"}

        if hasattr(execution, "__dict__"):
            return dict(execution.__dict__)

        if isinstance(execution, dict):
            return execution

        return {"status": "INCOMPLETE", "model": "NONE"}

    @staticmethod
    def _infer_sweep_watch_direction(htf_bias: Any) -> Direction:
        if htf_bias == Direction.LONG.value:
            return Direction.LONG
        if htf_bias == Direction.SHORT.value:
            return Direction.SHORT
        return Direction.NEUTRAL

    @staticmethod
    def _condition_names(items: list[Any]) -> list[str]:
        result: list[str] = []
        for item in items or []:
            name = getattr(item, "name", None)
            if name is not None:
                result.append(str(name))
        return result

    @staticmethod
    def _dedupe_keep_order(items: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            if item not in seen:
                seen.add(item)
                result.append(item)
        return result

    @staticmethod
    def _context_instrument(context: Any) -> Any:
        value = getattr(context, "instrument", None)
        enum_value = getattr(value, "value", None)
        if enum_value and str(enum_value).upper() != "UNKNOWN":
            return str(enum_value).upper()

        enum_name = getattr(value, "name", None)
        if enum_name and str(enum_name).upper() != "UNKNOWN":
            return str(enum_name).upper()

        for attr_name in ("symbol", "ticker"):
            fallback = getattr(context, attr_name, None)
            if fallback and str(fallback).upper() != "UNKNOWN":
                return str(fallback).upper()

        raw = getattr(context, "raw", None)
        if isinstance(raw, dict):
            for key in ("instrument", "symbol", "ticker"):
                fallback = raw.get(key)
                if fallback and str(fallback).upper() != "UNKNOWN":
                    return str(fallback).upper()

        return "UNKNOWN"

    @staticmethod
    def _context_price(context: Any) -> float | None:
        value = getattr(context, "current_price", None)
        if value is None:
            value = getattr(context, "price", None)
        return float(value) if value is not None else None

    @staticmethod
    def _enum_value(value: Any) -> Any:
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
    def _clamp_confidence(
        base: float,
        setup_conf: float | None,
        bonus: float = 0.0,
    ) -> float:
        value = base + bonus
        if setup_conf is not None:
            value = max(value, float(setup_conf))
        return round(min(max(value, 0.0), 1.0), 2)