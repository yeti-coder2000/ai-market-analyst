from __future__ import annotations

from typing import Any


POLICY_VERSION = "battle-gate-open-behavior-policy-v1.1-neutral-otd-transition"


def _s(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip().upper()


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _b(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _append_unique(items: list[str], value: str) -> None:
    if value and value not in items:
        items.append(value)


def evaluate_open_behavior_policy(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Shadow/v2 Battle Gate policy based on TPO open behavior.

    Core rule:
    - OPEN_TEST_DRIVE + HTF NEUTRAL is NOT counter-trend.
    - It is an allowed transition candidate from balance/accumulation into directional distribution,
      provided execution, stop, RR and market conditions are valid.

    This function intentionally returns a plain dict because battle_permission.py consumes:
    decision, risk_mode, battle_allowed, should_suppress_telegram, score_delta,
    reasons, blockers, modifiers, error.
    """

    reasons: list[str] = []
    blockers: list[str] = []
    modifiers: list[str] = []

    symbol = _s(payload.get("symbol"))
    direction = _s(payload.get("direction"))
    htf_bias = _s(payload.get("htf_bias"), "NEUTRAL")

    market_status = _s(payload.get("market_status"), "OPEN")
    tpo_permission = _s(payload.get("tpo_signal_permission"), "OPEN_FOR_EVALUATION")
    tpo_modifier = _s(payload.get("tpo_telegram_modifier"), "NEUTRAL")

    open_context = _s(
        payload.get("open_context")
        or payload.get("open_relation")
        or payload.get("tpo_open_context")
    )
    open_behavior = _s(
        payload.get("open_behavior")
        or payload.get("tpo_open_behavior")
    )

    entry_hint = _s(payload.get("entry_model_hint"))
    stop_hint = _s(payload.get("stop_model_hint"))
    battle_hint = _s(payload.get("battle_bias_hint"))

    execution_status = _s(payload.get("execution_status"))
    stop_quality = _s(payload.get("stop_quality"))
    practical_rr = _f(payload.get("practical_rr"), 0.0)

    min_practical_rr = _f(
        payload.get("min_practical_rr")
        or payload.get("minimum_practical_rr")
        or 2.0,
        2.0,
    )

    market_is_open = _b(payload.get("market_is_open"), market_status == "OPEN")

    decision = "RESEARCH_ONLY"
    risk_mode = "RESEARCH_COUNTERFACTUAL"
    battle_allowed = False
    should_suppress_telegram = True
    score_delta = 0.0

    # Hard market/data blockers.
    if market_status in {"MARKET_CLOSED", "CLOSED"} or not market_is_open:
        blockers.append("market_closed")
        reasons.append("Market is closed; no battle signal allowed.")
        return {
            "policy_version": POLICY_VERSION,
            "decision": "BLOCK",
            "risk_mode": "BLOCKED",
            "battle_allowed": False,
            "should_suppress_telegram": True,
            "score_delta": -3.0,
            "reasons": reasons,
            "blockers": blockers,
            "modifiers": modifiers,
            "error": None,
        }

    if market_status in {"STALE_DATA", "DATA_STALE"} or tpo_permission in {"STALE_DATA", "MARKET_CLOSED", "BLOCK"}:
        blockers.append("data_or_tpo_permission_block")
        reasons.append(f"TPO/market permission blocks the signal: market_status={market_status}, permission={tpo_permission}.")
        return {
            "policy_version": POLICY_VERSION,
            "decision": "BLOCK",
            "risk_mode": "BLOCKED",
            "battle_allowed": False,
            "should_suppress_telegram": True,
            "score_delta": -3.0,
            "reasons": reasons,
            "blockers": blockers,
            "modifiers": modifiers,
            "error": None,
        }

    # Common quality checks.
    executable = execution_status == "EXECUTABLE"
    stop_ok = stop_quality not in {"TIGHT_STOP", "BAD", "WEAK", "NO_STOP", "NONE", ""}
    rr_ok = practical_rr >= min_practical_rr

    htf_aligned = bool(direction and htf_bias and direction == htf_bias)
    htf_neutral = htf_bias in {"NEUTRAL", "NONE", "FLAT", ""}
    htf_conflict = bool(direction and htf_bias and not htf_neutral and direction != htf_bias)

    if htf_aligned:
        _append_unique(modifiers, "htf_aligned")
        reasons.append(f"Direction is aligned with HTF bias: {direction}.")
        score_delta += 0.75

    if htf_neutral:
        _append_unique(modifiers, "htf_neutral")
        reasons.append("HTF bias is NEUTRAL.")

    if htf_conflict:
        _append_unique(blockers, "htf_conflict")
        reasons.append(f"Direction conflicts with HTF bias: direction={direction}, htf_bias={htf_bias}.")
        score_delta -= 1.25

    if tpo_modifier == "BOOST":
        _append_unique(modifiers, "tpo_modifier_boost")
        reasons.append("TPO modifier is BOOST.")
        score_delta += 0.5
    elif tpo_modifier == "DOWNGRADE":
        _append_unique(modifiers, "tpo_modifier_downgrade")
        reasons.append("TPO modifier is DOWNGRADE.")
        score_delta -= 0.75

    if not executable:
        _append_unique(blockers, "execution_not_executable")
        reasons.append(f"Execution status is not EXECUTABLE: {execution_status or 'UNKNOWN'}.")
        score_delta -= 1.0

    if not stop_ok:
        _append_unique(blockers, "stop_quality_invalid")
        reasons.append(f"Stop quality is not valid for battle: {stop_quality or 'UNKNOWN'}.")
        score_delta -= 1.0

    if not rr_ok:
        _append_unique(blockers, "practical_rr_too_low")
        reasons.append(f"Practical RR is too low: {practical_rr:.2f} < {min_practical_rr:.2f}.")
        score_delta -= 1.0

    # Main open behavior policy.
    if open_behavior == "OPEN_TEST_DRIVE":
        _append_unique(modifiers, "open_behavior_open_test_drive")

        if htf_neutral:
            _append_unique(modifiers, "neutral_htf_open_test_drive_transition")
            reasons.append(
                "OPEN_TEST_DRIVE with NEUTRAL HTF is allowed as a transition candidate "
                "from balance/accumulation into directional distribution."
            )
            score_delta += 1.25

            if executable and stop_ok and rr_ok:
                decision = "ALLOW"
                risk_mode = "TRANSITION_CANDIDATE"
                battle_allowed = True
                should_suppress_telegram = False
            else:
                decision = "WATCH_PENDING"
                risk_mode = "LTF_OR_EXECUTION_PENDING"
                battle_allowed = False
                should_suppress_telegram = True

        elif htf_aligned:
            reasons.append(
                "OPEN_TEST_DRIVE supports a battle candidate with HTF alignment, executable setup, valid stop and RR."
            )
            score_delta += 1.0

            if executable and stop_ok and rr_ok:
                decision = "ALLOW"
                risk_mode = "BATTLE_CANDIDATE"
                battle_allowed = True
                should_suppress_telegram = False
            else:
                decision = "WATCH_PENDING"
                risk_mode = "LTF_OR_EXECUTION_PENDING"
                battle_allowed = False
                should_suppress_telegram = True

        else:
            reasons.append(
                "OPEN_TEST_DRIVE is present, but direction conflicts with HTF bias; keep as research/caution unless later confirmed exceptionally clean."
            )
            decision = "DOWNGRADE"
            risk_mode = "RESEARCH_COUNTERFACTUAL"
            battle_allowed = False
            should_suppress_telegram = True

    elif open_behavior == "OPEN_DRIVE":
        _append_unique(modifiers, "open_behavior_open_drive")
        reasons.append("OPEN_DRIVE can support directional battle only with HTF alignment and executable setup.")

        if htf_aligned and executable and stop_ok and rr_ok:
            decision = "ALLOW"
            risk_mode = "BATTLE_CANDIDATE"
            battle_allowed = True
            should_suppress_telegram = False
            score_delta += 1.25
        else:
            decision = "DOWNGRADE"
            risk_mode = "RESEARCH_COUNTERFACTUAL"
            battle_allowed = False
            should_suppress_telegram = True
            score_delta -= 0.25

    elif open_behavior == "OPEN_REJECTION_REVERSE":
        _append_unique(modifiers, "open_behavior_open_rejection_reverse")
        reasons.append("OPEN_REJECTION_REVERSE is allowed only as research/cautious setup unless LTF structure is very clean.")

        if htf_aligned and executable and stop_ok and rr_ok and tpo_modifier == "BOOST":
            decision = "ALLOW_WITH_CAUTION"
            risk_mode = "CAUTION_BATTLE_CANDIDATE"
            battle_allowed = True
            should_suppress_telegram = False
            score_delta += 0.25
        else:
            decision = "DOWNGRADE"
            risk_mode = "RESEARCH_COUNTERFACTUAL"
            battle_allowed = False
            should_suppress_telegram = True
            score_delta -= 0.5

    elif open_behavior == "OPEN_AUCTION":
        _append_unique(modifiers, "open_behavior_open_auction")
        reasons.append("OPEN_AUCTION = rotation/research environment, not directional battle by default.")
        decision = "RESEARCH_ONLY"
        risk_mode = "OBSERVE_ROTATION"
        battle_allowed = False
        should_suppress_telegram = True
        score_delta -= 1.0

    else:
        _append_unique(modifiers, "open_behavior_unconfirmed")
        reasons.append(f"Open behavior is not confirmed: {open_behavior or 'UNKNOWN'}.")
        decision = "RESEARCH_ONLY"
        risk_mode = "UNCONFIRMED_CONTEXT"
        battle_allowed = False
        should_suppress_telegram = True
        score_delta -= 0.75

    # Extra hints for observability.
    if entry_hint:
        _append_unique(modifiers, f"entry_hint_{entry_hint.lower()}")
    if stop_hint:
        _append_unique(modifiers, f"stop_hint_{stop_hint.lower()}")
    if battle_hint:
        _append_unique(modifiers, f"battle_hint_{battle_hint.lower()}")

    return {
        "policy_version": POLICY_VERSION,
        "decision": decision,
        "risk_mode": risk_mode,
        "battle_allowed": battle_allowed,
        "should_suppress_telegram": should_suppress_telegram,
        "score_delta": round(score_delta, 4),
        "reasons": reasons,
        "blockers": blockers,
        "modifiers": modifiers,
        "error": None,
        "diagnostics": {
            "symbol": symbol,
            "direction": direction,
            "htf_bias": htf_bias,
            "open_context": open_context,
            "open_behavior": open_behavior,
            "execution_status": execution_status,
            "stop_quality": stop_quality,
            "practical_rr": practical_rr,
            "min_practical_rr": min_practical_rr,
            "market_status": market_status,
            "tpo_signal_permission": tpo_permission,
            "tpo_telegram_modifier": tpo_modifier,
        },
    }