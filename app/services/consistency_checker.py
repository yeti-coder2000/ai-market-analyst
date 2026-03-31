from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional


@dataclass
class ConsistencyCheckResult:
    is_consistent: bool
    consistency_score: float
    conflict_flags: List[str]
    warnings: List[str]
    summary: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _norm(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def check_consistency(
    *,
    symbol: str,
    market_state: Optional[str],
    htf_bias: Optional[str],
    phase: Optional[str],
    final_signal_setup: Optional[str],
    final_signal_status: Optional[str],
    final_signal_direction: Optional[str],
    diagnostics: Dict[str, Dict[str, Any]],
    behavioral_summary: Optional[str] = None,
) -> ConsistencyCheckResult:
    """
    Rule-based consistency validator for:

        context -> setup diagnostics -> final signal -> behavioral summary

    Expected diagnostics shape:
    {
        "IMPULSE_PULLBACK_CONTINUATION": {
            "status": "IDLE",
            "direction": "SHORT",
            "confidence": 0.0,
            "passed": ["market_state"],
            "failed": ["impulse", "pullback", "htf_alignment"],
        },
        "SWEEP_RETURN_TO_VALUE": {
            "status": "READY",
            "direction": "SHORT",
            "confidence": 0.8,
            "passed": ["market_state", "sweep", "return_to_value"],
            "failed": [],
        },
    }
    """
    symbol = symbol.strip().upper()

    market_state_n = _norm(market_state)
    htf_bias_n = _norm(htf_bias)
    phase_n = _norm(phase)

    setup_n = _norm(final_signal_setup)
    status_n = _norm(final_signal_status)
    direction_n = _norm(final_signal_direction)
    behavioral_n = (behavioral_summary or "").strip().upper()

    conflict_flags: List[str] = []
    warnings: List[str] = []

    active_diag = diagnostics.get(setup_n, {}) if setup_n else {}
    diag_status_n = _norm(active_diag.get("status"))
    diag_direction_n = _norm(active_diag.get("direction"))
    diag_confidence = _to_float(active_diag.get("confidence"), 0.0)

    passed = {_norm(x) for x in active_diag.get("passed", [])}
    failed = {_norm(x) for x in active_diag.get("failed", [])}

    # ---------------------------------------------------------
    # 1. Final signal sanity
    # ---------------------------------------------------------
    if setup_n in {"", "NONE"} and status_n not in {"NO_SETUP", "IDLE", "WAIT", "NEUTRAL"}:
        conflict_flags.append("flag_none_setup_but_active_signal")

    if status_n == "READY" and setup_n in {"", "NONE"}:
        conflict_flags.append("flag_ready_without_setup")

    if status_n == "NO_SETUP" and direction_n not in {"", "NEUTRAL"}:
        conflict_flags.append("flag_no_setup_but_directional_bias")

    # ---------------------------------------------------------
    # 2. Final signal must align with selected diagnostic
    # ---------------------------------------------------------
    if setup_n not in {"", "NONE"}:
        if not active_diag:
            conflict_flags.append("flag_final_signal_setup_missing_in_diagnostics")
        else:
            if diag_status_n and status_n and diag_status_n != status_n:
                conflict_flags.append("flag_setup_status_mismatch")

            if diag_direction_n and direction_n and diag_direction_n != direction_n:
                conflict_flags.append("flag_setup_direction_mismatch")

    # ---------------------------------------------------------
    # 3. READY must be evidence-backed
    # ---------------------------------------------------------
    if status_n == "READY":
        if diag_confidence < 0.5:
            conflict_flags.append("flag_ready_with_low_confidence")

        if setup_n == "IMPULSE_PULLBACK_CONTINUATION":
            if "IMPULSE" in failed or "PULLBACK" in failed:
                conflict_flags.append("flag_continuation_ready_without_impulse_or_pullback")

        if setup_n == "SWEEP_RETURN_TO_VALUE":
            if "SWEEP" in failed or "RETURN_TO_VALUE" in failed:
                conflict_flags.append("flag_sweep_ready_without_sweep_or_return")

    # ---------------------------------------------------------
    # 4. Inactive states should not look aggressive
    # ---------------------------------------------------------
    if status_n in {"IDLE", "NO_SETUP"} and diag_confidence > 0.0:
        warnings.append("warn_inactive_setup_has_nonzero_confidence")

    # ---------------------------------------------------------
    # 5. HTF bias alignment
    # ---------------------------------------------------------
    if direction_n in {"LONG", "SHORT"} and htf_bias_n in {"LONG", "SHORT"}:
        if direction_n != htf_bias_n:
            # Counter-trend can be valid, but should usually be a reversal / sweep type.
            if setup_n != "SWEEP_RETURN_TO_VALUE":
                conflict_flags.append("flag_direction_vs_htf_bias_conflict")
            else:
                warnings.append("warn_counter_bias_reversal_setup")

    # ---------------------------------------------------------
    # 6. Market state / phase compatibility
    # ---------------------------------------------------------
    if setup_n == "IMPULSE_PULLBACK_CONTINUATION":
        if market_state_n not in {"TREND"}:
            conflict_flags.append("flag_continuation_in_wrong_market_state")
        if phase_n == "NO_STRUCTURE" and status_n == "READY":
            conflict_flags.append("flag_continuation_ready_in_no_structure_phase")

    if setup_n == "SWEEP_RETURN_TO_VALUE":
        if market_state_n not in {"TRANSITION", "BALANCE", "RANGE"}:
            warnings.append("warn_sweep_setup_in_unusual_market_state")

    # ---------------------------------------------------------
    # 7. Behavioral summary alignment
    # ---------------------------------------------------------
    if behavioral_n:
        if status_n == "READY":
            if any(x in behavioral_n for x in ["NO TRADE", "AVOID", "STAY OUT"]):
                conflict_flags.append("flag_behavior_summary_blocks_ready_signal")

        if status_n in {"NO_SETUP", "IDLE"}:
            if any(x in behavioral_n for x in ["STRONG ENTRY", "TAKE TRADE", "HIGH CONVICTION"]):
                conflict_flags.append("flag_behavior_summary_too_aggressive_for_inactive_signal")

    # ---------------------------------------------------------
    # 8. Known suspicious pattern from current logs
    # ---------------------------------------------------------
    if setup_n == "IMPULSE_PULLBACK_CONTINUATION" and status_n == "IDLE":
        if "MARKET_STATE" in passed and "HTF_ALIGNMENT" in failed:
            warnings.append("warn_htf_alignment_failed_despite_directional_match_check_logic")

    # ---------------------------------------------------------
    # Score
    # ---------------------------------------------------------
    score = 1.0
    score -= 0.20 * len(conflict_flags)
    score -= 0.05 * len(warnings)
    score = max(0.0, round(score, 2))

    is_consistent = len(conflict_flags) == 0

    if is_consistent and not warnings:
        summary = f"{symbol}: consistency OK"
    elif is_consistent and warnings:
        summary = f"{symbol}: consistency OK with warnings"
    else:
        summary = f"{symbol}: consistency conflicts detected"

    return ConsistencyCheckResult(
        is_consistent=is_consistent,
        consistency_score=score,
        conflict_flags=conflict_flags,
        warnings=warnings,
        summary=summary,
    )