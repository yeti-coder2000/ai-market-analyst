from __future__ import annotations

from typing import Any

from app.context.schema import (
    Direction,
    MarketState,
    SetupStatus,
    SetupType,
)
from app.scenarios.execution import build_execution_plan
from app.scenarios.schema import (
    ScenarioDecision,
    ScenarioEvidence,
    ScenarioPhase,
    ScenarioResult,
    ScenarioType,
)


class ScenarioEngine:
    """
    Rule-based scenario resolver.

    Goal of v1:
    - sit on top of stable MarketContext + setup rules
    - classify dominant market scenario
    - expose explainable decision state
    - remain deterministic and debuggable

    This engine does NOT replace setup rules.
    It interprets them in broader market context.
    """

    def run(self, context: Any, setups: list[Any]) -> ScenarioResult:
        setup_a = setups[0] if len(setups) > 0 else None
        setup_b = setups[1] if len(setups) > 1 else None

        evidence = self._build_evidence(context=context, setup_a=setup_a, setup_b=setup_b)

        # Priority 1: trend continuation scenarios
        trend_result = self._resolve_trend_continuation(
            context=context,
            setup_a=setup_a,
            evidence=evidence,
        )
        if trend_result is not None:
            return trend_result

        # Priority 2: sweep -> return to value scenarios
        sweep_result = self._resolve_sweep_return(
            context=context,
            setup_b=setup_b,
            evidence=evidence,
        )
        if sweep_result is not None:
            return sweep_result

        # Fallback
        return self._build_no_action_result(context=context, evidence=evidence)

    # ---------------------------------------------------------------------
    # Scenario resolvers
    # ---------------------------------------------------------------------

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

        # Fully confirmed trend continuation
        if setup_a_status == SetupStatus.READY.value:
            execution = build_execution_plan(
                context=context,
                scenario_type=scenario_type,
                direction=direction,
                evidence=evidence,
            )
            decision, status = self._resolve_confirmed_tradeability(execution)

            return ScenarioResult(
                instrument=getattr(context, "instrument"),
                price=self._context_price(context),
                scenario_type=scenario_type,
                phase=ScenarioPhase.CONFIRMED,
                decision=decision,
                market_state=getattr(context, "market_state"),
                direction=direction,
                status=status,
                setup_type=SetupType.IMPULSE_PULLBACK_CONTINUATION,
                dominant_setup="setup_a",
                setup_name=SetupType.IMPULSE_PULLBACK_CONTINUATION.value,
                rationale=(
                    "Trend continuation scenario confirmed: HTF bias, market regime, "
                    "impulse and pullback are aligned."
                ),
                confidence=self._clamp_confidence(
                    base=0.72,
                    setup_conf=evidence.setup_a_confidence,
                    bonus=0.10 if evidence.pullback_held_structure else 0.0,
                ),
                next_expected_event=(
                    "continuation_trigger"
                    if execution.status == "EXECUTABLE"
                    else "execution_completion"
                ),
                missing_conditions=(
                    []
                    if execution.status == "EXECUTABLE"
                    else ["execution_plan_incomplete"]
                ),
                alignment_score=self._infer_alignment_score(context, [setup_a]),
                tags=["trend", "continuation", direction.value.lower(), "confirmed"],
                evidence=evidence,
                execution=self._to_execution_payload(execution),
                metadata={
                    "engine_version": "scenario_engine_v1",
                    "resolver": "trend_continuation",
                    "execution_status": getattr(execution, "status", None),
                },
            )

        # Impulse exists, but pullback is not ready yet
        if evidence.impulse_detected and not evidence.pullback_detected:
            missing = ["pullback"]

            execution = build_execution_plan(
                context=context,
                scenario_type=scenario_type,
                direction=direction,
                evidence=evidence,
            )

            return ScenarioResult(
                instrument=getattr(context, "instrument"),
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
                    bonus=0.05 if evidence.impulse_detected else 0.0,
                ),
                next_expected_event="pullback_confirmation",
                missing_conditions=missing,
                alignment_score=self._infer_alignment_score(context, [setup_a]),
                tags=["trend", "impulse", "watch", direction.value.lower()],
                evidence=evidence,
                execution=self._to_execution_payload(execution),
                metadata={
                    "engine_version": "scenario_engine_v1",
                    "resolver": "trend_continuation",
                    "execution_status": getattr(execution, "status", None),
                },
            )

        # Trend context exists, but structure is not built yet
        missing = self._collect_missing_conditions([setup_a])

        execution = build_execution_plan(
            context=context,
            scenario_type=scenario_type,
            direction=direction,
            evidence=evidence,
        )

        return ScenarioResult(
            instrument=getattr(context, "instrument"),
            price=self._context_price(context),
            scenario_type=scenario_type,
            phase=ScenarioPhase.PRECONDITION,
            decision=ScenarioDecision.WATCH,
            market_state=getattr(context, "market_state"),
            direction=direction,
            status=SetupStatus.IDLE,
            setup_type=SetupType.IMPULSE_PULLBACK_CONTINUATION,
            dominant_setup="setup_a",
            setup_name=SetupType.IMPULSE_PULLBACK_CONTINUATION.value,
            rationale=(
                "Trend context exists, but continuation structure is not ready yet."
            ),
            confidence=self._clamp_confidence(
                base=0.35,
                setup_conf=evidence.setup_a_confidence,
                bonus=0.05 if market_state == MarketState.TREND.value else 0.0,
            ),
            next_expected_event=(
                "bullish_impulse" if direction == Direction.LONG else "bearish_impulse"
            ),
            missing_conditions=missing,
            alignment_score=self._infer_alignment_score(context, [setup_a]),
            tags=["trend", "precondition", direction.value.lower()],
            evidence=evidence,
            execution=self._to_execution_payload(execution),
            metadata={
                "engine_version": "scenario_engine_v1",
                "resolver": "trend_continuation",
                "execution_status": getattr(execution, "status", None),
            },
        )

    def _resolve_sweep_return(
        self,
        context: Any,
        setup_b: Any,
        evidence: ScenarioEvidence,
    ) -> ScenarioResult | None:
        market_state = evidence.market_state

        if market_state not in {
            MarketState.BALANCE.value,
            MarketState.TRANSITION.value,
        }:
            return None

        if not evidence.sweep_detected:
            return None

        if evidence.sweep_direction not in {Direction.LONG.value, Direction.SHORT.value}:
            return None

        direction = Direction(evidence.sweep_direction)
        scenario_type = (
            ScenarioType.SWEEP_RETURN_LONG
            if direction == Direction.LONG
            else ScenarioType.SWEEP_RETURN_SHORT
        )

        setup_b_status = evidence.setup_b_status

        if setup_b_status == SetupStatus.READY.value and evidence.return_to_value:
            execution = build_execution_plan(
                context=context,
                scenario_type=scenario_type,
                direction=direction,
                evidence=evidence,
            )
            decision, status = self._resolve_confirmed_tradeability(execution)

            return ScenarioResult(
                instrument=getattr(context, "instrument"),
                price=self._context_price(context),
                scenario_type=scenario_type,
                phase=ScenarioPhase.CONFIRMED,
                decision=decision,
                market_state=getattr(context, "market_state"),
                direction=direction,
                status=status,
                setup_type=SetupType.SWEEP_RETURN_TO_VALUE,
                dominant_setup="setup_b",
                setup_name=SetupType.SWEEP_RETURN_TO_VALUE.value,
                rationale=(
                    "Sweep-return scenario confirmed: liquidity sweep occurred and "
                    "price returned to value in non-trend conditions."
                ),
                confidence=self._clamp_confidence(
                    base=0.70,
                    setup_conf=evidence.setup_b_confidence,
                    bonus=0.08 if evidence.return_to_value else 0.0,
                ),
                next_expected_event=(
                    "entry_trigger"
                    if execution.status == "EXECUTABLE"
                    else "execution_completion"
                ),
                missing_conditions=(
                    []
                    if execution.status == "EXECUTABLE"
                    else ["execution_plan_incomplete"]
                ),
                alignment_score=self._infer_alignment_score(context, [setup_b]),
                tags=["sweep", "return_to_value", direction.value.lower(), "confirmed"],
                evidence=evidence,
                execution=self._to_execution_payload(execution),
                metadata={
                    "engine_version": "scenario_engine_v1",
                    "resolver": "sweep_return",
                    "execution_status": getattr(execution, "status", None),
                },
            )

        if evidence.sweep_detected and not evidence.return_to_value:
            missing = ["return_to_value"]

            execution = build_execution_plan(
                context=context,
                scenario_type=scenario_type,
                direction=direction,
                evidence=evidence,
            )

            return ScenarioResult(
                instrument=getattr(context, "instrument"),
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
                rationale=(
                    "Sweep detected, but price has not yet clearly returned to value."
                ),
                confidence=self._clamp_confidence(
                    base=0.50,
                    setup_conf=evidence.setup_b_confidence,
                    bonus=0.04,
                ),
                next_expected_event="return_to_value_confirmation",
                missing_conditions=missing,
                alignment_score=self._infer_alignment_score(context, [setup_b]),
                tags=["sweep", "watch", direction.value.lower()],
                evidence=evidence,
                execution=self._to_execution_payload(execution),
                metadata={
                    "engine_version": "scenario_engine_v1",
                    "resolver": "sweep_return",
                    "execution_status": getattr(execution, "status", None),
                },
            )

        missing = self._collect_missing_conditions([setup_b])
        if "return_to_value" not in missing:
            missing.append("return_to_value")

        execution = build_execution_plan(
            context=context,
            scenario_type=scenario_type,
            direction=direction,
            evidence=evidence,
        )

        return ScenarioResult(
            instrument=getattr(context, "instrument"),
            price=self._context_price(context),
            scenario_type=scenario_type,
            phase=ScenarioPhase.PRECONDITION,
            decision=ScenarioDecision.WATCH,
            market_state=getattr(context, "market_state"),
            direction=direction,
            status=SetupStatus.IDLE,
            setup_type=SetupType.SWEEP_RETURN_TO_VALUE,
            dominant_setup="setup_b",
            setup_name=SetupType.SWEEP_RETURN_TO_VALUE.value,
            rationale=(
                "Sweep context exists, but return-to-value setup is not fully built yet."
            ),
            confidence=self._clamp_confidence(
                base=0.34,
                setup_conf=evidence.setup_b_confidence,
                bonus=0.05 if evidence.sweep_detected else 0.0,
            ),
            next_expected_event="return_to_value_confirmation",
            missing_conditions=missing,
            alignment_score=self._infer_alignment_score(context, [setup_b]),
            tags=["sweep", "precondition", direction.value.lower()],
            evidence=evidence,
            execution=self._to_execution_payload(execution),
            metadata={
                "engine_version": "scenario_engine_v1",
                "resolver": "sweep_return",
                "execution_status": getattr(execution, "status", None),
            },
        )

    def _build_no_action_result(
        self,
        context: Any,
        evidence: ScenarioEvidence,
    ) -> ScenarioResult:
        missing = self._collect_missing_conditions([])
        scenario_type = ScenarioType.NO_ACTION
        direction = Direction.NEUTRAL

        execution = build_execution_plan(
            context=context,
            scenario_type=scenario_type,
            direction=direction,
            evidence=evidence,
        )

        return ScenarioResult(
            instrument=getattr(context, "instrument"),
            price=self._context_price(context),
            scenario_type=scenario_type,
            phase=ScenarioPhase.PRECONDITION,
            decision=ScenarioDecision.NO_TRADE,
            market_state=getattr(context, "market_state"),
            direction=direction,
            status=SetupStatus.NO_SETUP,
            setup_type=SetupType.NONE,
            dominant_setup=None,
            setup_name=None,
            rationale="No dominant scenario is currently confirmed.",
            confidence=0.10,
            next_expected_event=self._infer_next_expected_event(context),
            missing_conditions=missing,
            alignment_score=self._infer_alignment_score(context, []),
            tags=["no_action"],
            evidence=evidence,
            execution=self._to_execution_payload(execution),
            metadata={
                "engine_version": "scenario_engine_v1",
                "resolver": "fallback",
                "execution_status": getattr(execution, "status", None),
            },
        )

    # ---------------------------------------------------------------------
    # Evidence / scoring / helpers
    # ---------------------------------------------------------------------

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

        passed = []
        failed = []

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
            if status == SetupStatus.WATCH.value:
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

    # ---------------------------------------------------------------------
    # Low-level helpers
    # ---------------------------------------------------------------------

    @staticmethod
    def _resolve_confirmed_tradeability(execution: Any) -> tuple[ScenarioDecision, SetupStatus]:
        if getattr(execution, "status", None) == "EXECUTABLE":
            return ScenarioDecision.TRADEABLE, SetupStatus.READY
        return ScenarioDecision.WATCH, SetupStatus.WATCH

    @staticmethod
    def _to_execution_payload(execution: Any) -> dict[str, Any]:
        if execution is None:
            return {
                "status": "NOT_EXECUTABLE",
                "model": "NONE",
            }

        if hasattr(execution, "__dict__"):
            return dict(execution.__dict__)

        if isinstance(execution, dict):
            return execution

        return {
            "status": "INCOMPLETE",
            "model": "NONE",
        }

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