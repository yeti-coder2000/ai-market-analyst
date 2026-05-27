from __future__ import annotations

"""
Battle Gate v2 open-behavior policy for AI Market Analyst.

This module is intentionally pure and dependency-light.
It does not send Telegram, does not read/write runtime files, and does not mutate payloads.

Purpose:
    Convert the new TPO decision layer into Battle Gate permission modifiers.

Decision pipeline supported:
    HTF context
    → open type
    → first hour
    → open behavior
    → interest zone
    → 5m–15m entry model hint
    → stop hint
    → RR / practical RR
    → Battle Gate
    → Telegram

Public API:
    evaluate_open_behavior_policy(payload_or_metadata) -> dict

Recommended integration point:
    app/services/battle_permission.py

Important principle:
    POC / nPOC / VAH / VAL are interest zones, NOT entry triggers.
    No LTF model + no valid stop model = no battle alert.
"""

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


Decision = Literal["ALLOW", "BOOST", "DOWNGRADE", "RESEARCH_ONLY", "BLOCK"]
RiskMode = Literal["BATTLE_CANDIDATE", "RESEARCH_COUNTERFACTUAL", "BLOCKED"]


BLOCKING_MARKET_STATUSES = {
    "MARKET_CLOSED",
    "MARKET_CLOSED_AND_STALE",
    "STALE_DATA",
    "NO_DATA",
    "PROVIDER_ERROR",
}

BLOCKING_TPO_PERMISSIONS = {
    "MARKET_CLOSED",
    "STALE_DATA",
    "NO_DATA",
    "PROVIDER_ERROR",
    "BLOCKED_BY_CONTEXT",
    "BLOCKED_BY_AUCTION",
}

NO_DIRECTIONAL_ENTRY_HINTS = {
    "",
    "-",
    "NONE",
    "NO_ENTRY_MODEL",
    "NO_DIRECTIONAL_ENTRY_MODEL",
}

NO_STOP_HINTS = {
    "",
    "-",
    "NONE",
    "NO_STOP_MODEL",
}

INVALID_STOP_QUALITIES = {
    "TIGHT_STOP",
    "INVALID",
    "BAD",
}

DIRECTIONAL_BEHAVIORS = {
    "OPEN_DRIVE",
    "OPEN_TEST_DRIVE",
    "OPEN_REJECTION_REVERSE",
}

RESEARCH_BEHAVIORS = {
    "OPEN_AUCTION",
    "UNCONFIRMED",
    "UNKNOWN",
}

COUNTERTREND_OR_CAUTION_HINTS = {
    "RESEARCH_COUNTERTREND_UNLESS_LTF_CONFIRMED",
    "RESEARCH_UNTIL_ACCEPTANCE_CONFIRMED",
}

DOWNGRADE_HINTS = {
    "DOWNGRADE_NO_DIRECTIONAL_BATTLE",
}

ALLOW_HINTS = {
    "ALLOW_IF_HTF_ALIGNED_AND_LTF_CONFIRMED",
    "BOOST_IF_HTF_ALIGNED_AND_EXECUTABLE",
}

NPOC_WARNING = "npoc_is_interest_zone_not_entry_trigger"


@dataclass(frozen=True)
class OpenBehaviorPolicyResult:
    decision: Decision
    risk_mode: RiskMode
    battle_allowed: bool
    should_suppress_telegram: bool
    score_delta: float = 0.0
    reasons: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    modifiers: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _as_upper(value: Any, default: str = "") -> str:
    text = str(value or "").strip().upper()
    return text or default


def _as_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_nested(payload: dict[str, Any], dotted: str) -> Any:
    cur: Any = payload
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def first_non_empty(payload: dict[str, Any], paths: list[str], default: Any = None) -> Any:
    for path in paths:
        value = _get_nested(payload, path)
        if value not in (None, "", [], {}):
            return value
    return default


def _collect_warnings(payload: dict[str, Any]) -> list[str]:
    candidates = [
        first_non_empty(payload, ["open_behavior.warnings"], []),
        first_non_empty(payload, ["metadata.open_behavior.warnings"], []),
        first_non_empty(payload, ["metadata.auction_filters.open_behavior.warnings"], []),
        first_non_empty(payload, ["auction_filters.open_behavior.warnings"], []),
        first_non_empty(payload, ["open_behavior_warnings"], []),
    ]

    result: list[str] = []
    for item in candidates:
        if isinstance(item, list):
            result.extend(str(x) for x in item if str(x).strip())
        elif isinstance(item, str) and item.strip():
            result.append(item.strip())

    return sorted(set(result))


def extract_open_behavior_inputs(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Extract Battle Gate v2 fields from root, metadata, auction_filters, and open_behavior blocks.

    This supports payloads where stateful_batch_runner duplicates TPO fields in several places.
    """
    if not isinstance(payload, dict):
        payload = {}

    return {
        "symbol": first_non_empty(payload, ["symbol", "metadata.symbol", "instrument", "metadata.instrument"], "-"),
        "direction": _as_upper(first_non_empty(payload, ["direction", "metadata.direction"], "NEUTRAL"), "NEUTRAL"),
        "htf_bias": _as_upper(first_non_empty(payload, ["htf_bias", "metadata.htf_bias"], "NEUTRAL"), "NEUTRAL"),
        "market_state": _as_upper(first_non_empty(payload, ["market_state", "metadata.market_state"], "UNKNOWN"), "UNKNOWN"),
        "market_status": _as_upper(
            first_non_empty(
                payload,
                [
                    "market_status",
                    "metadata.market_status",
                    "metadata.auction_context.market_status",
                    "auction_context.market_status",
                    "auction_filters.market_status",
                    "metadata.auction_filters.market_status",
                ],
                "UNKNOWN",
            ),
            "UNKNOWN",
        ),
        "tpo_signal_permission": _as_upper(
            first_non_empty(
                payload,
                [
                    "tpo_signal_permission",
                    "metadata.tpo_signal_permission",
                    "metadata.auction_filters.tpo_signal_permission",
                    "auction_filters.tpo_signal_permission",
                ],
                "UNKNOWN",
            ),
            "UNKNOWN",
        ),
        "tpo_telegram_modifier": _as_upper(
            first_non_empty(
                payload,
                [
                    "tpo_telegram_modifier",
                    "telegram_modifier",
                    "metadata.tpo_telegram_modifier",
                    "metadata.auction_filters.telegram_modifier",
                    "metadata.auction_filters.tpo_telegram_modifier",
                    "auction_filters.telegram_modifier",
                    "auction_filters.tpo_telegram_modifier",
                ],
                "NEUTRAL",
            ),
            "NEUTRAL",
        ),
        "open_context": _as_upper(
            first_non_empty(
                payload,
                [
                    "open_context",
                    "metadata.open_context",
                    "metadata.auction_context.open_context",
                    "metadata.auction_filters.open_context",
                    "auction_context.open_context",
                    "auction_filters.open_context",
                    "open_behavior.open_context",
                    "metadata.open_behavior.open_context",
                ],
                "UNKNOWN",
            ),
            "UNKNOWN",
        ),
        "open_behavior": _as_upper(
            first_non_empty(
                payload,
                [
                    "open_behavior",
                    "metadata.open_behavior_value",
                    "metadata.open_behavior",
                    "metadata.auction_context.open_behavior",
                    "metadata.auction_filters.open_behavior",
                    "auction_context.open_behavior",
                    "auction_filters.open_behavior",
                    "open_behavior.open_behavior",
                    "metadata.open_behavior.open_behavior",
                ],
                "UNKNOWN",
            ),
            "UNKNOWN",
        ),
        "open_behavior_confidence": _as_float(
            first_non_empty(
                payload,
                [
                    "open_behavior_confidence",
                    "metadata.open_behavior_confidence",
                    "metadata.auction_context.open_behavior_confidence",
                    "metadata.auction_filters.open_behavior_confidence",
                    "auction_context.open_behavior_confidence",
                    "auction_filters.open_behavior_confidence",
                    "open_behavior.open_behavior_confidence",
                    "metadata.open_behavior.open_behavior_confidence",
                ],
                None,
            ),
            None,
        ),
        "entry_model_hint": _as_upper(
            first_non_empty(
                payload,
                [
                    "entry_model_hint",
                    "metadata.entry_model_hint",
                    "metadata.auction_context.entry_model_hint",
                    "metadata.auction_filters.entry_model_hint",
                    "auction_context.entry_model_hint",
                    "auction_filters.entry_model_hint",
                    "open_behavior.entry_model_hint",
                    "metadata.open_behavior.entry_model_hint",
                ],
                "NO_ENTRY_MODEL",
            ),
            "NO_ENTRY_MODEL",
        ),
        "stop_model_hint": _as_upper(
            first_non_empty(
                payload,
                [
                    "stop_model_hint",
                    "metadata.stop_model_hint",
                    "metadata.auction_context.stop_model_hint",
                    "metadata.auction_filters.stop_model_hint",
                    "auction_context.stop_model_hint",
                    "auction_filters.stop_model_hint",
                    "open_behavior.stop_model_hint",
                    "metadata.open_behavior.stop_model_hint",
                ],
                "NO_STOP_MODEL",
            ),
            "NO_STOP_MODEL",
        ),
        "battle_bias_hint": _as_upper(
            first_non_empty(
                payload,
                [
                    "battle_bias_hint",
                    "metadata.battle_bias_hint",
                    "metadata.auction_context.battle_bias_hint",
                    "metadata.auction_filters.battle_bias_hint",
                    "auction_context.battle_bias_hint",
                    "auction_filters.battle_bias_hint",
                    "open_behavior.battle_bias_hint",
                    "metadata.open_behavior.battle_bias_hint",
                ],
                "RESEARCH_ONLY",
            ),
            "RESEARCH_ONLY",
        ),
        "primary_interest_zone": first_non_empty(
            payload,
            [
                "primary_interest_zone",
                "metadata.primary_interest_zone",
                "metadata.auction_context.primary_interest_zone",
                "metadata.auction_filters.primary_interest_zone",
                "auction_context.primary_interest_zone",
                "auction_filters.primary_interest_zone",
                "open_behavior.primary_interest_zone",
                "metadata.open_behavior.primary_interest_zone",
            ],
            {},
        ),
        "warnings": _collect_warnings(payload),
        "execution_status": _as_upper(first_non_empty(payload, ["execution_status", "metadata.execution_status"], "UNKNOWN"), "UNKNOWN"),
        "execution_model": _as_upper(first_non_empty(payload, ["execution_model", "metadata.execution_model"], "UNKNOWN"), "UNKNOWN"),
        "stop_quality": _as_upper(first_non_empty(payload, ["stop_quality", "metadata.stop_quality"], "UNKNOWN"), "UNKNOWN"),
        "practical_rr": _as_float(first_non_empty(payload, ["practical_rr", "metadata.practical_rr"], None), None),
        "risk_reward_ratio": _as_float(first_non_empty(payload, ["risk_reward_ratio", "metadata.risk_reward_ratio"], None), None),
    }


def _htf_aligned(direction: str, htf_bias: str) -> bool:
    if direction not in {"LONG", "SHORT"}:
        return False
    if htf_bias not in {"LONG", "SHORT"}:
        return False
    return direction == htf_bias


def evaluate_open_behavior_policy(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Evaluate new open-behavior constraints for Battle Gate v2.

    The function is conservative by design:
    - Blocks stale/closed/degraded market states.
    - Downgrades or research-only for OPEN_AUCTION / UNCONFIRMED behavior.
    - Requires entry model and stop model hints before allowing battle.
    - nPOC warning never blocks by itself, but forces LTF confirmation/research handling.
    """
    data = extract_open_behavior_inputs(payload)

    reasons: list[str] = []
    blockers: list[str] = []
    modifiers: list[str] = []
    score_delta = 0.0

    market_status = data["market_status"]
    tpo_permission = data["tpo_signal_permission"]
    tpo_modifier = data["tpo_telegram_modifier"]
    open_context = data["open_context"]
    open_behavior = data["open_behavior"]
    confidence = data["open_behavior_confidence"]
    entry_hint = data["entry_model_hint"]
    stop_hint = data["stop_model_hint"]
    battle_hint = data["battle_bias_hint"]
    direction = data["direction"]
    htf_bias = data["htf_bias"]
    execution_status = data["execution_status"]
    stop_quality = data["stop_quality"]
    practical_rr = data["practical_rr"]
    rr = data["risk_reward_ratio"]
    warnings = data["warnings"]

    aligned = _htf_aligned(direction, htf_bias)

    # Hard market/data blockers.
    if market_status in BLOCKING_MARKET_STATUSES:
        blockers.append(f"market_status_{market_status.lower()}")
        reasons.append(f"Market/data status blocks battle: {market_status}.")

    if tpo_permission in BLOCKING_TPO_PERMISSIONS:
        blockers.append(f"tpo_permission_{tpo_permission.lower()}")
        reasons.append(f"TPO permission blocks battle: {tpo_permission}.")

    if blockers:
        return OpenBehaviorPolicyResult(
            decision="BLOCK",
            risk_mode="BLOCKED",
            battle_allowed=False,
            should_suppress_telegram=True,
            score_delta=-2.0,
            reasons=reasons,
            blockers=blockers,
            modifiers=modifiers,
        ).to_dict()

    # Open behavior unknown/auction means no directional battle by default.
    if open_behavior in RESEARCH_BEHAVIORS:
        modifiers.append(f"open_behavior_{open_behavior.lower()}")
        reasons.append(f"Open behavior is {open_behavior}; directional battle requires later LTF confirmation.")
        score_delta -= 0.75

    if open_behavior == "OPEN_AUCTION":
        reasons.append("OPEN_AUCTION = rotation/research environment, not directional battle by default.")

    if battle_hint in DOWNGRADE_HINTS:
        modifiers.append(f"battle_hint_{battle_hint.lower()}")
        reasons.append(f"Battle hint downgrades directional alert: {battle_hint}.")
        score_delta -= 0.75

    if battle_hint in COUNTERTREND_OR_CAUTION_HINTS:
        modifiers.append(f"battle_hint_{battle_hint.lower()}")
        reasons.append(f"Battle hint requires LTF confirmation/research handling: {battle_hint}.")
        score_delta -= 0.5

    # Entry/stop hints are required for battle candidate.
    if entry_hint in NO_DIRECTIONAL_ENTRY_HINTS:
        modifiers.append("missing_directional_entry_model")
        reasons.append("No directional 5m–15m entry model hint; battle alert disabled until LTF model appears.")
        score_delta -= 0.75

    if stop_hint in NO_STOP_HINTS:
        modifiers.append("missing_stop_model")
        reasons.append("No stop model hint; battle alert disabled until invalidation is defined.")
        score_delta -= 0.75

    if stop_quality in INVALID_STOP_QUALITIES:
        modifiers.append(f"stop_quality_{stop_quality.lower()}")
        reasons.append(f"Stop quality is not acceptable: {stop_quality}.")
        score_delta -= 1.0

    # RR constraints: practical RR is preferred, theoretical RR is fallback.
    rr_value = practical_rr if practical_rr is not None else rr
    if rr_value is not None and rr_value < 2.0:
        modifiers.append("rr_below_2")
        reasons.append(f"RR/practical RR below battle threshold: {rr_value:.2f}.")
        score_delta -= 0.75

    # nPOC / POC interest-zone handling.
    if NPOC_WARNING in warnings:
        modifiers.append("npoc_interest_zone_requires_ltf_confirmation")
        reasons.append("nPOC is an interest zone, not an entry trigger; require LTF confirmation.")
        score_delta -= 0.25

    # Positive conditions.
    if open_behavior in {"OPEN_DRIVE", "OPEN_TEST_DRIVE"}:
        reasons.append(f"{open_behavior} can support a battle candidate only with HTF alignment, executable setup and valid stop.")
        score_delta += 0.5

    if open_behavior == "OPEN_REJECTION_REVERSE":
        reasons.append("OPEN_REJECTION_REVERSE is allowed only as research/cautious setup unless LTF structure is very clean.")
        score_delta -= 0.25

    if aligned:
        modifiers.append("htf_aligned")
        reasons.append(f"Direction is aligned with HTF bias: {direction}.")
        score_delta += 0.5
    else:
        modifiers.append("htf_not_aligned")
        reasons.append(f"Direction is not HTF-aligned: direction={direction}, htf_bias={htf_bias}.")
        score_delta -= 0.5

    if tpo_modifier == "BOOST":
        modifiers.append("tpo_modifier_boost")
        reasons.append("TPO modifier is BOOST.")
        score_delta += 0.35
    elif tpo_modifier == "DOWNGRADE":
        modifiers.append("tpo_modifier_downgrade")
        reasons.append("TPO modifier is DOWNGRADE.")
        score_delta -= 0.75

    executable = execution_status == "EXECUTABLE"
    has_entry = entry_hint not in NO_DIRECTIONAL_ENTRY_HINTS
    has_stop = stop_hint not in NO_STOP_HINTS
    good_stop = stop_quality not in INVALID_STOP_QUALITIES
    good_rr = rr_value is None or rr_value >= 2.0
    behavior_can_battle = open_behavior in {"OPEN_DRIVE", "OPEN_TEST_DRIVE"} or (
        open_behavior == "OPEN_REJECTION_REVERSE" and aligned and battle_hint not in DOWNGRADE_HINTS
    )

    # Final conservative decision.
    if (
        behavior_can_battle
        and has_entry
        and has_stop
        and good_stop
        and good_rr
        and aligned
        and tpo_modifier != "DOWNGRADE"
        and battle_hint in ALLOW_HINTS
        and executable
    ):
        return OpenBehaviorPolicyResult(
            decision="ALLOW" if tpo_modifier != "BOOST" else "BOOST",
            risk_mode="BATTLE_CANDIDATE",
            battle_allowed=True,
            should_suppress_telegram=False,
            score_delta=round(score_delta, 4),
            reasons=reasons,
            blockers=[],
            modifiers=modifiers,
        ).to_dict()

    # Otherwise keep the event in research/counterfactual tracking.
    if tpo_modifier == "DOWNGRADE" or open_behavior in RESEARCH_BEHAVIORS or not has_entry or not has_stop:
        decision: Decision = "RESEARCH_ONLY"
    else:
        decision = "DOWNGRADE"

    return OpenBehaviorPolicyResult(
        decision=decision,
        risk_mode="RESEARCH_COUNTERFACTUAL",
        battle_allowed=False,
        should_suppress_telegram=True,
        score_delta=round(score_delta, 4),
        reasons=reasons,
        blockers=[],
        modifiers=modifiers,
    ).to_dict()


def apply_open_behavior_policy_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Return a shallow-copied payload with Battle Gate v2 open-behavior policy fields attached.

    Useful before telemetry/statistics so the new logic is visible even before it
    fully controls legacy Battle Gate decisions.
    """
    output = dict(payload) if isinstance(payload, dict) else {}
    policy = evaluate_open_behavior_policy(output)

    output["battle_gate_v2_policy"] = policy
    output["battle_gate_v2_decision"] = policy.get("decision")
    output["battle_gate_v2_risk_mode"] = policy.get("risk_mode")
    output["battle_gate_v2_score_delta"] = policy.get("score_delta")
    output["battle_gate_v2_battle_allowed"] = policy.get("battle_allowed")
    output["battle_gate_v2_reasons"] = policy.get("reasons")
    output["battle_gate_v2_blockers"] = policy.get("blockers")
    output["battle_gate_v2_modifiers"] = policy.get("modifiers")
    return output
