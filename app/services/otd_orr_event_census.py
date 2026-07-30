from __future__ import annotations

"""Causal OTD/ORR event census for Backtest Integrity v2.0.

The execution backtest answers whether the current LTF limit-entry model was
ready, filled and profitable.  This module answers a different question:
whether the underlying auction event developed after it was confirmed.

Development is measured from the close of the confirmation bar to the
structural invalidation extreme already known at confirmation.  No future bar
is used to define the reference price or risk unit.  Event development and
trade performance remain separate throughout the report.
"""

from collections import Counter, defaultdict
from datetime import datetime, timedelta
import math
from statistics import median
from typing import Any, Mapping, Sequence

import pandas as pd

from app.services.ltf_execution_backtest import (
    HistoricalWatchCandidate,
    build_dynamic_context_timeline,
    normalize_m5_history,
)


OTD_ORR_EVENT_CENSUS_VERSION = (
    "backtest-integrity-v2.0.3-otd-orr-event-census"
)
DEFAULT_PRIMARY_DEVELOPMENT_R = 1.5
DEFAULT_DEVELOPMENT_THRESHOLDS_R = (1.0, 1.5, 2.0)
ENTRY_MODEL_NAME = "LTF_V2_LIMIT_ON_RETEST"
SUPPORTED_EVENT_FAMILIES = {
    "OPEN_TEST_DRIVE",
    "OPEN_REJECTION_REVERSE",
}


def _as_utc(value: Any) -> datetime:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("invalid UTC timestamp")
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.to_pydatetime()


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _median(values: Sequence[float]) -> float | None:
    return median(values) if values else None


def _mean(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _wilson_interval(
    successes: int,
    failures: int,
    z: float = 1.96,
) -> list[float] | None:
    total = successes + failures
    if total <= 0:
        return None
    proportion = successes / total
    denominator = 1.0 + (z * z / total)
    center = (
        proportion + z * z / (2.0 * total)
    ) / denominator
    margin = (
        z
        * math.sqrt(
            (proportion * (1.0 - proportion) / total)
            + (z * z / (4.0 * total * total))
        )
        / denominator
    )
    return [
        max(0.0, center - margin),
        min(1.0, center + margin),
    ]


def _threshold_key(value: float) -> str:
    number = float(value)
    rounded_to_two_places = round(number, 2)
    if number == rounded_to_two_places:
        return f"{number:.2f}"
    return format(number, ".17g")


def _validated_thresholds(
    primary_development_r: float,
    thresholds_r: Sequence[float],
) -> tuple[float, ...]:
    primary = float(primary_development_r)
    if not math.isfinite(primary) or primary <= 0:
        raise ValueError("primary_development_r must be a finite positive number")
    values = {primary}
    for value in thresholds_r:
        number = float(value)
        if not math.isfinite(number) or number <= 0:
            raise ValueError(
                "development thresholds must be finite positive numbers"
            )
        values.add(number)
    return tuple(sorted(values))


def _empty_event_record(
    candidate: HistoricalWatchCandidate,
    *,
    primary_development_r: float,
    thresholds_r: Sequence[float],
) -> dict[str, Any]:
    threshold_hits = {
        _threshold_key(value): None for value in thresholds_r
    }
    htf_alignment_state = str(
        candidate.payload.get("signal_alignment") or ""
    ).upper()
    synthetic_open_confirmed = candidate.payload.get(
        "synthetic_open_confirmed"
    )
    requested_execution_eligibility = bool(
        candidate.payload.get(
            "event_census_execution_eligible",
            htf_alignment_state != "COUNTER_TREND",
        )
    )
    execution_eligible = (
        requested_execution_eligibility
        and htf_alignment_state != "COUNTER_TREND"
        and synthetic_open_confirmed is True
    )
    if htf_alignment_state == "COUNTER_TREND":
        execution_exclusion_reason = "COUNTER_TREND_HARD_GATE"
    elif synthetic_open_confirmed is not True:
        execution_exclusion_reason = "UNCONFIRMED_SYNTHETIC_OPEN"
    else:
        execution_exclusion_reason = candidate.payload.get(
            "event_census_execution_exclusion_reason"
        )
    return {
        "event_census_version": OTD_ORR_EVENT_CENSUS_VERSION,
        "candidate_id": candidate.candidate_id,
        "symbol": candidate.symbol,
        "session_id": candidate.session_id,
        "reference_profile_id": candidate.reference_profile_id,
        "setup_family": candidate.setup_family,
        "direction": candidate.direction,
        "session_scope": candidate.payload.get("session_scope"),
        "open_location": candidate.payload.get("open_location"),
        "htf_bias": candidate.htf_bias,
        "htf_alignment_state": candidate.payload.get("signal_alignment"),
        "macro_context_status": candidate.payload.get(
            "macro_guard_status"
        )
        or "NOT_RECONSTRUCTED",
        "weekly_cot_asof_cohort": "NO_DATA",
        "weekly_cot_asof_status": (
            "NOT_JOINED_CAUSAL_PUBLICATION_TIMESTAMP_REQUIRED"
        ),
        "operational_positioning_asof_cohort": "NO_DATA",
        "operational_positioning_asof_status": (
            "NOT_JOINED_PERSISTED_SNAPSHOT_TIMELINE_REQUIRED"
        ),
        "profile_reliability": candidate.payload.get(
            "profile_reliability"
        )
        or "RECONSTRUCTED_NOT_PARITY_VERIFIED",
        "execution_universe_eligible": execution_eligible,
        "execution_universe_exclusion_reason": execution_exclusion_reason,
        "synthetic_open_confirmed": synthetic_open_confirmed,
        "session_open_utc": candidate.session_open_utc.isoformat(),
        "confirmed_at_utc": candidate.activated_at_utc.isoformat(),
        "expires_at_utc": candidate.expires_at_utc.isoformat(),
        "minutes_open_to_confirmation": round(
            (
                candidate.activated_at_utc - candidate.session_open_utc
            ).total_seconds()
            / 60.0,
            4,
        ),
        "confirmation_reference_price": None,
        "confirmation_reference_source": "ACTIVATION_BAR_CLOSE",
        "structural_invalidation_type": "TEST_EXTREME_AT_CONFIRMATION",
        "structural_invalidation_price": candidate.test_extreme,
        "structural_r_price": None,
        "primary_development_R": primary_development_r,
        "event_evaluable": False,
        "event_evaluation_status": "PENDING",
        "primary_development_reached": False,
        "primary_development_ambiguous": False,
        "event_outcome": "NOT_EVALUATED",
        "event_mfe_R": None,
        "event_mae_R": None,
        "mae_R_until_primary_development": None,
        "threshold_hits_utc": threshold_hits,
        "ambiguous_thresholds_R": [],
        "primary_developed_at_utc": None,
        "minutes_confirmation_to_primary_development": None,
        "minutes_open_to_primary_development": None,
        "observation_end_utc": None,
        "terminal_reason": None,
        "forward_m5_integrity_status": "PENDING",
        "excursion_observation_complete": False,
        "excursion_terminal_bar_policy": (
            "EXCLUDE_TERMINAL_BAR_WHEN_INTRABAR_ORDER_IS_UNKNOWN"
        ),
    }


def measure_event_development(
    candidate: HistoricalWatchCandidate,
    frame: pd.DataFrame,
    *,
    primary_development_r: float = DEFAULT_PRIMARY_DEVELOPMENT_R,
    thresholds_r: Sequence[
        float
    ] = DEFAULT_DEVELOPMENT_THRESHOLDS_R,
) -> dict[str, Any]:
    """Measure auction development independently from execution-model results.

    Same-bar threshold and invalidation/context cancellation is unresolvable
    with OHLC data.  Such a threshold is marked ambiguous and is not counted as
    reached.  This is deliberately conservative.
    """

    thresholds = _validated_thresholds(
        primary_development_r,
        thresholds_r,
    )
    record = _empty_event_record(
        candidate,
        primary_development_r=float(primary_development_r),
        thresholds_r=thresholds,
    )
    if candidate.setup_family not in SUPPORTED_EVENT_FAMILIES:
        record.update(
            {
                "event_evaluation_status": "UNSUPPORTED_EVENT_FAMILY",
                "event_outcome": "NOT_EVALUABLE_UNSUPPORTED_FAMILY",
            }
        )
        return record
    if candidate.payload.get("synthetic_open_confirmed") is not True:
        record.update(
            {
                "event_evaluation_status": (
                    "UNCONFIRMED_SYNTHETIC_OPEN"
                ),
                "event_outcome": (
                    "NOT_EVALUABLE_UNCONFIRMED_SYNTHETIC_OPEN"
                ),
                "terminal_reason": "UNCONFIRMED_SYNTHETIC_OPEN",
                "forward_m5_integrity_status": "NOT_EVALUATED",
            }
        )
        return record

    history = normalize_m5_history(frame, symbol=candidate.symbol)
    closes = pd.to_datetime(history["bar_close_utc"], utc=True)
    activation_rows = history.loc[
        (closes <= pd.Timestamp(candidate.activated_at_utc))
        & (closes > pd.Timestamp(candidate.session_open_utc))
    ]
    if activation_rows.empty:
        record.update(
            {
                "event_evaluation_status": "MISSING_CONFIRMATION_BAR",
                "event_outcome": "NOT_EVALUABLE_MISSING_CONFIRMATION_BAR",
            }
        )
        return record

    activation_row = activation_rows.iloc[-1]
    activation_close = _as_utc(activation_row["bar_close_utc"])
    if activation_close != candidate.activated_at_utc:
        record.update(
            {
                "event_evaluation_status": "MISSING_EXACT_CONFIRMATION_BAR",
                "event_outcome": (
                    "NOT_EVALUABLE_MISSING_EXACT_CONFIRMATION_BAR"
                ),
            }
        )
        return record
    if bool(activation_row.get("_source_duplicate_bar_open_utc", False)):
        record.update(
            {
                "event_evaluation_status": "DUPLICATE_CONFIRMATION_BAR",
                "event_outcome": (
                    "NOT_EVALUABLE_DUPLICATE_CONFIRMATION_BAR"
                ),
            }
        )
        return record

    reference_price = float(activation_row["close"])
    invalidation_price = float(candidate.test_extreme)
    structural_r = abs(reference_price - invalidation_price)
    geometry_is_valid = (
        structural_r > 0
        and math.isfinite(structural_r)
        and (
            invalidation_price < reference_price
            if candidate.direction == "LONG"
            else invalidation_price > reference_price
        )
    )
    record.update(
        {
            "confirmation_reference_price": reference_price,
            "structural_r_price": structural_r,
        }
    )
    if not geometry_is_valid:
        record.update(
            {
                "event_evaluation_status": "INVALID_STRUCTURAL_GEOMETRY",
                "event_outcome": "NOT_EVALUABLE_INVALID_GEOMETRY",
            }
        )
        return record

    forward = history.loc[
        (closes > pd.Timestamp(candidate.activated_at_utc))
        & (closes <= pd.Timestamp(candidate.expires_at_utc))
    ]
    if forward.empty:
        record.update(
            {
                "event_evaluation_status": "NO_FORWARD_CLOSED_BARS",
                "event_outcome": "NOT_EVALUABLE_NO_FORWARD_BARS",
            }
        )
        return record

    timeline = build_dynamic_context_timeline(candidate, history)
    cancellation_by_close = {
        point.as_of_utc: point.cancellation_reason
        for point in timeline
        if point.cancellation_reason
    }
    threshold_hits: dict[str, str | None] = {
        _threshold_key(value): None for value in thresholds
    }
    primary_key = _threshold_key(float(primary_development_r))
    ambiguous_thresholds: set[float] = set()
    event_mfe_r = 0.0
    event_mae_r = 0.0
    mae_until_primary: float | None = None
    terminal_reason: str | None = None
    forward_m5_integrity_status = "COMPLETE"
    observation_end = candidate.activated_at_utc
    expected_bar_close = candidate.activated_at_utc + timedelta(minutes=5)

    for _, row in forward.iterrows():
        bar_close = _as_utc(row["bar_close_utc"])
        if bar_close != expected_bar_close:
            terminal_reason = "INCOMPLETE_FORWARD_M5_SEQUENCE"
            forward_m5_integrity_status = (
                "INCOMPLETE_FORWARD_M5_SEQUENCE"
            )
            break
        if bool(row.get("_source_duplicate_bar_open_utc", False)):
            terminal_reason = "DUPLICATE_FORWARD_M5_BAR"
            forward_m5_integrity_status = "DUPLICATE_FORWARD_M5_BAR"
            break
        expected_bar_close = bar_close + timedelta(minutes=5)

        high = float(row["high"])
        low = float(row["low"])
        favorable_price = (
            max(0.0, high - reference_price)
            if candidate.direction == "LONG"
            else max(0.0, reference_price - low)
        )
        adverse_price = (
            max(0.0, reference_price - low)
            if candidate.direction == "LONG"
            else max(0.0, high - reference_price)
        )

        invalidated = (
            low <= invalidation_price
            if candidate.direction == "LONG"
            else high >= invalidation_price
        )
        context_reason = cancellation_by_close.get(bar_close)
        terminal_on_bar = invalidated or context_reason is not None
        newly_touched = [
            threshold
            for threshold in thresholds
            if threshold_hits[_threshold_key(threshold)] is None
            and favorable_price >= threshold * structural_r
        ]

        if terminal_on_bar:
            ambiguous_thresholds.update(newly_touched)
            terminal_reason = (
                "INVALIDATED_BY_STRUCTURAL_EXTREME"
                if invalidated
                else str(context_reason)
            )
            observation_end = bar_close
            break

        event_mfe_r = max(event_mfe_r, favorable_price / structural_r)
        event_mae_r = max(event_mae_r, adverse_price / structural_r)
        observation_end = bar_close
        for threshold in newly_touched:
            threshold_hits[_threshold_key(threshold)] = bar_close.isoformat()
            if (
                _threshold_key(threshold) == primary_key
                and mae_until_primary is None
            ):
                # Whole-bar adverse excursion is retained.  M5 OHLC cannot
                # determine whether it occurred before or after the threshold.
                mae_until_primary = event_mae_r
    else:
        if observation_end < candidate.expires_at_utc:
            terminal_reason = "RIGHT_CENSORED_BEFORE_SESSION_HORIZON"
            forward_m5_integrity_status = (
                "RIGHT_CENSORED_BEFORE_SESSION_HORIZON"
            )
        else:
            terminal_reason = "SESSION_HORIZON_EXPIRED"

    primary_hit = threshold_hits[primary_key]
    primary_ambiguous = any(
        _threshold_key(threshold) == primary_key
        for threshold in ambiguous_thresholds
    )
    incomplete_forward_data = forward_m5_integrity_status != "COMPLETE"
    developed = primary_hit is not None
    event_evaluable = developed or not incomplete_forward_data
    if incomplete_forward_data and not developed:
        primary_ambiguous = False
        event_mfe_value: float | None = None
        event_mae_value: float | None = None
        mae_until_primary = None
        event_evaluation_status = forward_m5_integrity_status
        event_outcome = (
            "NOT_EVALUABLE_RIGHT_CENSORED"
            if terminal_reason == "RIGHT_CENSORED_BEFORE_SESSION_HORIZON"
            else f"NOT_EVALUABLE_{terminal_reason}"
        )
    elif developed:
        event_mfe_value = event_mfe_r
        event_mae_value = event_mae_r
        event_evaluation_status = (
            "EVALUATED_CAUSALLY"
            if not incomplete_forward_data
            else (
                "EVALUATED_CAUSALLY_WITH_INCOMPLETE_"
                "POST_DEVELOPMENT_OBSERVATION"
            )
        )
        event_outcome = "DEVELOPED"
    elif primary_ambiguous:
        event_mfe_value = event_mfe_r
        event_mae_value = event_mae_r
        event_evaluation_status = "EVALUATED_CAUSALLY"
        event_outcome = "AMBIGUOUS_DEVELOPMENT_AND_TERMINAL_SAME_BAR"
    elif terminal_reason == "INVALIDATED_BY_STRUCTURAL_EXTREME":
        event_mfe_value = event_mfe_r
        event_mae_value = event_mae_r
        event_evaluation_status = "EVALUATED_CAUSALLY"
        event_outcome = "INVALIDATED_BEFORE_DEVELOPMENT"
    elif terminal_reason == "SESSION_HORIZON_EXPIRED":
        event_mfe_value = event_mfe_r
        event_mae_value = event_mae_r
        event_evaluation_status = "EVALUATED_CAUSALLY"
        event_outcome = "EXPIRED_NO_DEVELOPMENT"
    else:
        event_mfe_value = event_mfe_r
        event_mae_value = event_mae_r
        event_evaluation_status = "EVALUATED_CAUSALLY"
        event_outcome = "CONTEXT_CANCELLED_BEFORE_DEVELOPMENT"

    primary_time = _as_utc(primary_hit) if primary_hit else None
    record.update(
        {
            "event_evaluable": event_evaluable,
            "event_evaluation_status": event_evaluation_status,
            "primary_development_reached": developed,
            "primary_development_ambiguous": primary_ambiguous,
            "event_outcome": event_outcome,
            "event_mfe_R": event_mfe_value,
            "event_mae_R": event_mae_value,
            "mae_R_until_primary_development": (
                mae_until_primary if developed else None
            ),
            "threshold_hits_utc": threshold_hits,
            "ambiguous_thresholds_R": sorted(ambiguous_thresholds),
            "primary_developed_at_utc": primary_hit,
            "minutes_confirmation_to_primary_development": (
                round(
                    (
                        primary_time - candidate.activated_at_utc
                    ).total_seconds()
                    / 60.0,
                    4,
                )
                if primary_time is not None
                else None
            ),
            "minutes_open_to_primary_development": (
                round(
                    (
                        primary_time - candidate.session_open_utc
                    ).total_seconds()
                    / 60.0,
                    4,
                )
                if primary_time is not None
                else None
            ),
            "observation_end_utc": observation_end.isoformat(),
            "terminal_reason": terminal_reason,
            "forward_m5_integrity_status": forward_m5_integrity_status,
            "excursion_observation_complete": not incomplete_forward_data,
        }
    )
    return record


def _execution_fields(
    event: Mapping[str, Any],
    execution: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if execution is None:
        execution_eligible = bool(
            event.get("execution_universe_eligible")
        )
        return {
            "execution_record_available": False,
            "entry_model": (
                ENTRY_MODEL_NAME
                if execution_eligible
                else "NOT_APPLICABLE_HARD_GATE"
            ),
            "ready": False,
            "ready_at_utc": None,
            "filled": False,
            "filled_at_utc": None,
            "execution_outcome": (
                "MISSING_EXECUTION_RECORD"
                if execution_eligible
                else "EXCLUDED_FROM_EXECUTION_UNIVERSE"
            ),
            "gross_R": None,
            "net_R": None,
            "total_cost_R": None,
            "practical_rr_bucket": "NOT_READY",
            "retest_depth_ATR": None,
            "stop_distance_ATR": None,
            "minutes_open_to_ready": None,
            "minutes_confirmation_to_ready": None,
            "minutes_open_to_fill": None,
            "minutes_confirmation_to_fill": None,
            "execution_cost_source": None,
            "execution_m5_integrity_status": "NOT_APPLICABLE",
            "ready_evaluable": not execution_eligible,
            "fill_evaluable": not execution_eligible,
            "trade_outcome_evaluable": not execution_eligible,
        }

    session_open = _as_utc(event["session_open_utc"])
    confirmed_at = _as_utc(event["confirmed_at_utc"])
    ready_at = (
        _as_utc(execution["ready_at_utc"])
        if execution.get("ready_at_utc")
        else None
    )
    filled_at = (
        _as_utc(execution["filled_at_utc"])
        if execution.get("filled_at_utc")
        else None
    )
    cost_model = execution.get("execution_cost_model")
    cost_source = (
        cost_model.get("source")
        if isinstance(cost_model, Mapping)
        else None
    )
    return {
        "execution_record_available": True,
        "entry_model": ENTRY_MODEL_NAME,
        "ready": bool(execution.get("ready")),
        "ready_at_utc": (
            ready_at.isoformat() if ready_at is not None else None
        ),
        "filled": filled_at is not None,
        "filled_at_utc": (
            filled_at.isoformat() if filled_at is not None else None
        ),
        "execution_outcome": execution.get("outcome"),
        "gross_R": execution.get("gross_R"),
        "net_R": execution.get("net_R"),
        "total_cost_R": execution.get("total_cost_R"),
        "practical_rr_bucket": execution.get(
            "practical_rr_bucket"
        )
        or "NOT_READY",
        "retest_depth_ATR": execution.get("retest_depth_ATR"),
        "stop_distance_ATR": execution.get("stop_distance_ATR"),
        "minutes_open_to_ready": (
            round(
                (ready_at - session_open).total_seconds() / 60.0,
                4,
            )
            if ready_at is not None
            else None
        ),
        "minutes_confirmation_to_ready": (
            round(
                (ready_at - confirmed_at).total_seconds() / 60.0,
                4,
            )
            if ready_at is not None
            else None
        ),
        "minutes_open_to_fill": (
            round(
                (filled_at - session_open).total_seconds() / 60.0,
                4,
            )
            if filled_at is not None
            else None
        ),
        "minutes_confirmation_to_fill": (
            round(
                (filled_at - confirmed_at).total_seconds() / 60.0,
                4,
            )
            if filled_at is not None
            else None
        ),
        "execution_cost_source": cost_source,
        "execution_m5_integrity_status": execution.get(
            "execution_m5_integrity_status"
        )
        or "NOT_REPORTED",
        "ready_evaluable": execution.get("ready_evaluable") is not False,
        "fill_evaluable": execution.get("fill_evaluable") is not False,
        "trade_outcome_evaluable": (
            execution.get("trade_outcome_evaluable") is not False
        ),
    }


def _event_execution_identity_mismatches(
    event: Mapping[str, Any],
    execution: Mapping[str, Any],
) -> list[str]:
    mismatches: list[str] = []
    for field in ("symbol", "setup_family", "direction"):
        event_value = str(event.get(field) or "").upper()
        execution_value = str(execution.get(field) or "").upper()
        if not event_value or event_value != execution_value:
            mismatches.append(field)

    event_confirmed = event.get("confirmed_at_utc")
    execution_activated = execution.get("activated_at_utc")
    if not event_confirmed or not execution_activated:
        mismatches.append("activated_at_utc")
    else:
        try:
            timestamps_match = (
                _as_utc(event_confirmed)
                == _as_utc(execution_activated)
            )
        except (TypeError, ValueError):
            timestamps_match = False
        if not timestamps_match:
            mismatches.append("activated_at_utc")
    return mismatches


def join_event_and_execution_records(
    event_records: Sequence[Mapping[str, Any]],
    execution_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    execution_by_id: dict[str, Mapping[str, Any]] = {}
    for row in execution_rows:
        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id:
            raise ValueError("execution row is missing candidate_id")
        if candidate_id in execution_by_id:
            raise ValueError(
                f"duplicate execution candidate_id={candidate_id}"
            )
        execution_by_id[candidate_id] = row

    event_ids: set[str] = set()
    missing_eligible_execution: list[str] = []
    joined: list[dict[str, Any]] = []
    for event in sorted(
        event_records,
        key=lambda row: (
            _as_utc(row["confirmed_at_utc"]),
            str(row["candidate_id"]),
        ),
    ):
        candidate_id = str(event.get("candidate_id") or "")
        if not candidate_id:
            raise ValueError("event record is missing candidate_id")
        if candidate_id in event_ids:
            raise ValueError(f"duplicate event candidate_id={candidate_id}")
        event_ids.add(candidate_id)
        htf_alignment_state = str(
            event.get("htf_alignment_state") or ""
        ).upper()
        execution_eligible = bool(
            event.get("execution_universe_eligible")
        )
        if htf_alignment_state == "COUNTER_TREND" and execution_eligible:
            raise ValueError(
                "COUNTER_TREND events must remain execution-ineligible; "
                f"candidate_id={candidate_id}; "
                "semantic hard gates override derived eligibility flags"
            )
        execution = execution_by_id.get(candidate_id)
        if execution is not None:
            identity_mismatches = _event_execution_identity_mismatches(
                event,
                execution,
            )
            if identity_mismatches:
                raise ValueError(
                    "event/execution identity mismatch; "
                    f"candidate_id={candidate_id}; "
                    f"fields={identity_mismatches}"
                )
        if (
            execution is not None
            and not execution_eligible
        ):
            hard_gate = str(
                event.get("execution_universe_exclusion_reason")
                or htf_alignment_state
                or "HARD_GATE"
            )
            raise ValueError(
                f"{hard_gate} execution-ineligible event cannot have "
                "an execution row; "
                f"candidate_id={candidate_id}; "
                "hard-gated events must remain outside execution replay"
            )
        if (
            execution is None
            and execution_eligible
        ):
            missing_eligible_execution.append(candidate_id)
        combined = dict(event)
        combined.update(
            _execution_fields(
                event,
                execution,
            )
        )
        joined.append(combined)

    execution_ids = set(execution_by_id)
    if not execution_ids.issubset(event_ids):
        missing_events = sorted(execution_ids - event_ids)
        raise ValueError(
            "event/execution candidate coverage mismatch; "
            f"missing_events={missing_events[:5]}; "
            "execution rows cannot exist outside the event universe"
        )
    if missing_eligible_execution:
        raise ValueError(
            "event/execution candidate coverage mismatch; "
            "eligible events are missing execution rows; "
            f"candidate_ids={missing_eligible_execution[:5]}"
        )
    return joined


def _numeric_values(
    rows: Sequence[Mapping[str, Any]],
    field: str,
) -> list[float]:
    values: list[float] = []
    for row in rows:
        number = _finite_float(row.get(field))
        if number is not None:
            values.append(number)
    return values


def _validate_primary_threshold_alignment(
    rows: Sequence[Mapping[str, Any]],
    *,
    primary_development_r: float,
) -> None:
    requested = float(primary_development_r)
    for row in rows:
        measured = _finite_float(row.get("primary_development_R"))
        if (
            measured is None
            or _threshold_key(measured) != _threshold_key(requested)
        ):
            raise ValueError(
                "primary development threshold mismatch; "
                f"candidate_id={row.get('candidate_id')}; "
                f"record_primary_R={measured}; "
                f"report_primary_R={requested}"
            )


def summarize_event_census(
    rows: Sequence[Mapping[str, Any]],
    *,
    primary_development_r: float,
) -> dict[str, Any]:
    records = list(rows)
    _validate_primary_threshold_alignment(
        records,
        primary_development_r=primary_development_r,
    )
    evaluable = [
        row for row in records if bool(row.get("event_evaluable"))
    ]
    unambiguous = [
        row
        for row in evaluable
        if not bool(row.get("primary_development_ambiguous"))
    ]
    developed = [
        row
        for row in unambiguous
        if bool(row.get("primary_development_reached"))
    ]
    known_failures = [
        row
        for row in unambiguous
        if not bool(row.get("primary_development_reached"))
    ]
    ambiguous_events = [
        row
        for row in evaluable
        if bool(row.get("primary_development_ambiguous"))
    ]
    not_evaluable = [
        row for row in records if not bool(row.get("event_evaluable"))
    ]
    excursion_complete = [
        row
        for row in evaluable
        if bool(row.get("excursion_observation_complete"))
    ]
    failures = len(known_failures)
    execution_eligible = [
        row
        for row in records
        if bool(row.get("execution_universe_eligible"))
    ]
    ready_evaluable = [
        row
        for row in execution_eligible
        if row.get("ready_evaluable") is not False
    ]
    ready = [
        row for row in ready_evaluable if bool(row.get("ready"))
    ]
    fill_evaluable_ready = [
        row
        for row in ready
        if row.get("fill_evaluable") is not False
    ]
    filled = [
        row for row in fill_evaluable_ready if bool(row.get("filled"))
    ]
    outcome_evaluable_filled = [
        row
        for row in filled
        if row.get("trade_outcome_evaluable") is not False
    ]
    trade_wins = [
        row
        for row in outcome_evaluable_filled
        if row.get("execution_outcome") == "TP_HIT"
    ]
    trade_losses = [
        row
        for row in outcome_evaluable_filled
        if str(row.get("execution_outcome") or "").startswith("SL_HIT")
    ]
    closed = len(trade_wins) + len(trade_losses)
    gross_values = _numeric_values(outcome_evaluable_filled, "gross_R")
    net_values = _numeric_values(outcome_evaluable_filled, "net_R")
    execution_integrity_statuses = Counter(
        str(row.get("execution_m5_integrity_status") or "NOT_REPORTED")
        for row in execution_eligible
    )
    threshold_keys = sorted(
        {
            str(key)
            for row in records
            for key in (
                row.get("threshold_hits_utc", {}).keys()
                if isinstance(row.get("threshold_hits_utc"), Mapping)
                else []
            )
        },
        key=float,
    )
    threshold_summary: dict[str, dict[str, Any]] = {}
    for key in threshold_keys:
        threshold_measured = [
            row
            for row in records
            if isinstance(row.get("threshold_hits_utc"), Mapping)
            and key in row["threshold_hits_utc"]
        ]
        threshold_ambiguous = [
            row
            for row in threshold_measured
            if any(
                _threshold_key(float(value)) == key
                for value in row.get("ambiguous_thresholds_R") or []
            )
        ]
        threshold_ambiguous_ids = {
            str(row["candidate_id"]) for row in threshold_ambiguous
        }
        threshold_reached = [
            row
            for row in threshold_measured
            if row["threshold_hits_utc"].get(key) is not None
        ]
        threshold_known_failures = [
            row
            for row in threshold_measured
            if str(row["candidate_id"]) not in threshold_ambiguous_ids
            and row["threshold_hits_utc"].get(key) is None
            and bool(row.get("excursion_observation_complete"))
        ]
        eligible = len(threshold_reached) + len(
            threshold_known_failures
        )
        reached = len(threshold_reached)
        ambiguous_count = len(threshold_ambiguous)
        threshold_summary[key] = {
            "reached_count": reached,
            "ambiguous_count": ambiguous_count,
            "eligible_count": eligible,
            "not_measured_count": (
                len(records) - len(threshold_measured)
            ),
            "development_rate": reached / eligible if eligible else None,
        }

    event_outcomes = Counter(
        str(row.get("event_outcome") or "UNKNOWN") for row in records
    )
    terminal_reasons = Counter(
        str(row.get("terminal_reason") or "NONE") for row in records
    )
    profile_reliability = Counter(
        str(row.get("profile_reliability") or "UNKNOWN")
        for row in records
    )
    cost_sources = Counter(
        str(row.get("execution_cost_source") or "UNKNOWN")
        for row in records
    )
    invalidation_types = Counter(
        str(row.get("structural_invalidation_type") or "UNKNOWN")
        for row in records
    )
    forward_m5_integrity = Counter(
        str(row.get("forward_m5_integrity_status") or "UNKNOWN")
        for row in records
    )
    weekly_cot_cohorts = Counter(
        str(row.get("weekly_cot_asof_cohort") or "NO_DATA")
        for row in records
    )
    operational_cohorts = Counter(
        str(
            row.get("operational_positioning_asof_cohort")
            or "NO_DATA"
        )
        for row in records
    )
    developed_ids = {
        str(row["candidate_id"]) for row in developed
    }
    filled_ids = {str(row["candidate_id"]) for row in filled}
    tp_ids = {str(row["candidate_id"]) for row in trade_wins}
    sl_ids = {str(row["candidate_id"]) for row in trade_losses}
    known_failure_ids = {
        str(row["candidate_id"]) for row in known_failures
    }
    ambiguous_ids = {
        str(row["candidate_id"]) for row in ambiguous_events
    }
    not_evaluable_ids = {
        str(row["candidate_id"]) for row in not_evaluable
    }

    return {
        "event_count": len(records),
        "event_evaluable_count": len(evaluable),
        "event_not_evaluable_count": len(records) - len(evaluable),
        "development_ambiguous_count": len(evaluable) - len(unambiguous),
        "development_denominator": len(unambiguous),
        "developed_count": len(developed),
        "development_failure_count": failures,
        "primary_development_R": float(primary_development_r),
        "development_rate": (
            len(developed) / len(unambiguous) if unambiguous else None
        ),
        "development_wilson_95": _wilson_interval(
            len(developed),
            failures,
        ),
        "minimum_sample_warning": len(unambiguous) < 30,
        "thresholds": threshold_summary,
        "median_event_MFE_R": _median(
            _numeric_values(excursion_complete, "event_mfe_R")
        ),
        "median_event_MAE_R": _median(
            _numeric_values(excursion_complete, "event_mae_R")
        ),
        "median_MAE_R_until_primary_development": _median(
            _numeric_values(
                developed,
                "mae_R_until_primary_development",
            )
        ),
        "median_minutes_open_to_confirmation": _median(
            _numeric_values(records, "minutes_open_to_confirmation")
        ),
        "median_minutes_confirmation_to_primary_development": _median(
            _numeric_values(
                developed,
                "minutes_confirmation_to_primary_development",
            )
        ),
        "median_minutes_open_to_primary_development": _median(
            _numeric_values(
                developed,
                "minutes_open_to_primary_development",
            )
        ),
        "event_outcomes": dict(sorted(event_outcomes.items())),
        "terminal_reasons": dict(sorted(terminal_reasons.items())),
        "structural_invalidation": {
            "basis_counts": dict(sorted(invalidation_types.items())),
            "invalidated_before_development_count": event_outcomes.get(
                "INVALIDATED_BEFORE_DEVELOPMENT",
                0,
            ),
            "comparison_status": (
                "SINGLE_FIXED_BASIS_NO_ALTERNATIVE_COMPARISON"
            ),
        },
        "execution": {
            "entry_model": ENTRY_MODEL_NAME,
            "eligible_event_count": len(execution_eligible),
            "excluded_event_count": (
                len(records) - len(execution_eligible)
            ),
            "exclusion_reasons": dict(
                sorted(
                    Counter(
                        str(
                            row.get("execution_universe_exclusion_reason")
                            or "UNSPECIFIED"
                        )
                        for row in records
                        if not bool(
                            row.get("execution_universe_eligible")
                        )
                    ).items()
                )
            ),
            "ready_evaluable_event_count": len(ready_evaluable),
            "ready_count": len(ready),
            "ready_rate": (
                len(ready) / len(ready_evaluable)
                if ready_evaluable
                else None
            ),
            "fill_evaluable_ready_count": len(fill_evaluable_ready),
            "filled_count": len(filled),
            "fill_rate_of_ready": (
                len(filled) / len(fill_evaluable_ready)
                if fill_evaluable_ready
                else None
            ),
            "trade_outcome_evaluable_filled_count": len(
                outcome_evaluable_filled
            ),
            "trade_outcome_unknown_filled_count": (
                len(filled) - len(outcome_evaluable_filled)
            ),
            "m5_integrity_status_counts": dict(
                sorted(execution_integrity_statuses.items())
            ),
            "closed_trade_count": closed,
            "tp_count": len(trade_wins),
            "sl_count": len(trade_losses),
            "trade_winrate_closed": (
                len(trade_wins) / closed if closed else None
            ),
            "trade_winrate_wilson_95": _wilson_interval(
                len(trade_wins),
                len(trade_losses),
            ),
            "average_gross_R_filled": _mean(gross_values),
            "average_net_R_filled": _mean(net_values),
            "gross_total_R": sum(gross_values),
            "net_total_R": sum(net_values),
            "median_retest_depth_ATR": _median(
                _numeric_values(ready, "retest_depth_ATR")
            ),
            "retest_depth_sample_count": len(
                _numeric_values(ready, "retest_depth_ATR")
            ),
            "median_stop_distance_ATR": _median(
                _numeric_values(ready, "stop_distance_ATR")
            ),
            "median_minutes_open_to_ready": _median(
                _numeric_values(ready, "minutes_open_to_ready")
            ),
            "median_minutes_confirmation_to_ready": _median(
                _numeric_values(
                    ready,
                    "minutes_confirmation_to_ready",
                )
            ),
            "median_minutes_open_to_fill": _median(
                _numeric_values(filled, "minutes_open_to_fill")
            ),
            "median_minutes_confirmation_to_fill": _median(
                _numeric_values(
                    filled,
                    "minutes_confirmation_to_fill",
                )
            ),
        },
        "development_vs_execution": {
            "developed_and_filled_count": len(
                developed_ids & filled_ids
            ),
            "developed_without_fill_count": len(
                developed_ids - filled_ids
            ),
            "filled_without_primary_development_count": len(
                filled_ids & known_failure_ids
            ),
            "filled_with_known_primary_development_failure_count": len(
                filled_ids & known_failure_ids
            ),
            "filled_with_ambiguous_primary_development_count": len(
                filled_ids & ambiguous_ids
            ),
            "filled_with_not_evaluable_event_count": len(
                filled_ids & not_evaluable_ids
            ),
            "developed_and_tp_count": len(developed_ids & tp_ids),
            "developed_and_sl_count": len(developed_ids & sl_ids),
            "developed_execution_ineligible_count": sum(
                1
                for row in developed
                if not bool(row.get("execution_universe_eligible"))
            ),
        },
        "data_reliability": {
            "profile_reliability_counts": dict(
                sorted(profile_reliability.items())
            ),
            "forward_m5_integrity_status_counts": dict(
                sorted(forward_m5_integrity.items())
            ),
            "excursion_observation_complete_count": len(
                excursion_complete
            ),
            "excursion_observation_incomplete_count": (
                len(evaluable) - len(excursion_complete)
            ),
            "synthetic_open_confirmed_true_count": sum(
                1
                for row in records
                if row.get("synthetic_open_confirmed") is True
            ),
            "synthetic_open_confirmed_false_count": sum(
                1
                for row in records
                if row.get("synthetic_open_confirmed") is False
            ),
            "synthetic_open_confirmed_unknown_count": sum(
                1
                for row in records
                if row.get("synthetic_open_confirmed") is None
            ),
            "execution_cost_source_counts": dict(
                sorted(cost_sources.items())
            ),
            "missing_execution_record_count": sum(
                1
                for row in records
                if not bool(row.get("execution_record_available"))
            ),
            "expected_execution_exclusion_count": sum(
                1
                for row in records
                if not bool(row.get("execution_universe_eligible"))
                and not bool(row.get("execution_record_available"))
            ),
            "unexpected_missing_execution_record_count": sum(
                1
                for row in records
                if bool(row.get("execution_universe_eligible"))
                and not bool(row.get("execution_record_available"))
            ),
            "weekly_cot_asof_cohort_counts": dict(
                sorted(weekly_cot_cohorts.items())
            ),
            "operational_positioning_asof_cohort_counts": dict(
                sorted(operational_cohorts.items())
            ),
        },
    }


def _grouped_summaries(
    records: Sequence[Mapping[str, Any]],
    *,
    key_fields: Sequence[str],
    primary_development_r: float,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in records:
        key = "|".join(
            str(row.get(field) or "UNKNOWN") for field in key_fields
        )
        grouped[key].append(row)
    return {
        key: summarize_event_census(
            values,
            primary_development_r=primary_development_r,
        )
        for key, values in sorted(grouped.items())
    }


def _chronological_holdout_start_utc(
    records: Sequence[Mapping[str, Any]],
    *,
    holdout_fraction: float,
) -> datetime | None:
    """Return one time boundary derived from the full event universe."""

    ordered = sorted(
        records,
        key=lambda row: (
            _as_utc(row["confirmed_at_utc"]),
            str(row["candidate_id"]),
        ),
    )
    if len(ordered) < 2:
        return None
    target_split = max(
        1,
        min(
            len(ordered) - 1,
            int(round(len(ordered) * (1.0 - holdout_fraction))),
        ),
    )
    timestamps = [
        _as_utc(row["confirmed_at_utc"])
        for row in ordered
    ]
    cutoff = timestamps[target_split]
    while target_split > 0 and timestamps[target_split - 1] == cutoff:
        target_split -= 1
    if target_split == 0:
        target_split = 1
        while (
            target_split < len(timestamps)
            and timestamps[target_split] == timestamps[0]
        ):
            target_split += 1
        if target_split >= len(timestamps):
            return None
        cutoff = timestamps[target_split]
    return cutoff


def compile_event_census(
    *,
    event_records: Sequence[Mapping[str, Any]],
    execution_rows: Sequence[Mapping[str, Any]],
    holdout_fraction: float = 0.30,
    primary_development_r: float = DEFAULT_PRIMARY_DEVELOPMENT_R,
) -> dict[str, Any]:
    if not 0.10 <= holdout_fraction <= 0.50:
        raise ValueError("holdout_fraction must be between 0.10 and 0.50")
    joined = join_event_and_execution_records(
        event_records,
        execution_rows,
    )
    holdout_start = _chronological_holdout_start_utc(
        joined,
        holdout_fraction=holdout_fraction,
    )
    if holdout_start is None:
        development = joined
        holdout: list[dict[str, Any]] = []
    else:
        development = [
            row
            for row in joined
            if _as_utc(row["confirmed_at_utc"]) < holdout_start
        ]
        holdout = [
            row
            for row in joined
            if _as_utc(row["confirmed_at_utc"]) >= holdout_start
        ]
    bounded_split = len(development)
    measured_thresholds = sorted(
        {
            float(key)
            for row in joined
            for key in (
                row.get("threshold_hits_utc", {}).keys()
                if isinstance(row.get("threshold_hits_utc"), Mapping)
                else []
            )
        }
    )
    if not measured_thresholds:
        measured_thresholds = list(
            _validated_thresholds(
                primary_development_r,
                DEFAULT_DEVELOPMENT_THRESHOLDS_R,
            )
        )
    return {
        "version": OTD_ORR_EVENT_CENSUS_VERSION,
        "status": "OK",
        "definition": {
            "event_reference": "CONFIRMATION_BAR_CLOSE",
            "structural_r": (
                "ABS(CONFIRMATION_BAR_CLOSE-TEST_EXTREME_AT_CONFIRMATION)"
            ),
            "primary_development_R": float(primary_development_r),
            "thresholds_R": measured_thresholds,
            "threshold_schema_policy": (
                "UNMEASURED_THRESHOLD_EXCLUDED_FROM_DENOMINATOR"
            ),
            "same_bar_development_and_terminal_policy": (
                "AMBIGUOUS_EXCLUDED_FROM_DEVELOPMENT_DENOMINATOR"
            ),
            "terminal_bar_excursion_policy": (
                "EXCLUDE_TERMINAL_BAR_WHEN_INTRABAR_ORDER_IS_UNKNOWN"
            ),
            "forward_m5_sequence_policy": (
                "PRESERVE_PROVEN_DEVELOPMENT_FAIL_CLOSED_IF_UNPROVEN"
            ),
            "excursion_observation_policy": (
                "INCOMPLETE_POST_DEVELOPMENT_HORIZONS_EXCLUDED_FROM_"
                "MFE_MAE_AGGREGATES"
            ),
            "development_rate_is_trade_winrate": False,
            "event_universe": (
                "ALL_RECONSTRUCTED_OTD_ORR; UNCONFIRMED_SYNTHETIC_"
                "OPENS_EXCLUDED_FROM_DEVELOPMENT_DENOMINATOR"
            ),
            "execution_universe": (
                "COUNTER_TREND_AND_UNCONFIRMED_SYNTHETIC_OPEN_"
                "HARD_GATES_PRESERVED"
            ),
            "chronological_holdout_cutoff": (
                "ONE_CONFIRMED_AT_UTC_BOUNDARY_FROM_FULL_EVENT_UNIVERSE"
            ),
        },
        "integrity": {
            "look_ahead_allowed": False,
            "confirmation_reference_is_causal": True,
            "future_extreme_does_not_define_risk": True,
            "incomplete_context_blocks_are_excluded": True,
            "event_and_execution_denominators_are_separate": True,
            "event_and_execution_share_holdout_cutoff": True,
            "counter_trend_events_can_receive_execution": False,
            "unconfirmed_synthetic_open_can_receive_execution": False,
            "weekly_cot_event_join": (
                "NO_DATA_UNTIL_CAUSAL_PUBLICATION_TIMESTAMPS_EXIST"
            ),
            "operational_positioning_event_join": (
                "NO_DATA_UNTIL_PERSISTED_ASOF_SNAPSHOTS_COVER_EVENT"
            ),
            "research_only": True,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
        "split_index": bounded_split,
        "holdout_fraction": holdout_fraction,
        "holdout_start_utc": (
            holdout_start.isoformat() if holdout_start else None
        ),
        "metrics": {
            "all": summarize_event_census(
                joined,
                primary_development_r=primary_development_r,
            ),
            "development": summarize_event_census(
                development,
                primary_development_r=primary_development_r,
            ),
            "holdout": summarize_event_census(
                holdout,
                primary_development_r=primary_development_r,
            ),
            "by_symbol": _grouped_summaries(
                joined,
                key_fields=("symbol",),
                primary_development_r=primary_development_r,
            ),
            "by_family": _grouped_summaries(
                joined,
                key_fields=("setup_family",),
                primary_development_r=primary_development_r,
            ),
            "by_direction": _grouped_summaries(
                joined,
                key_fields=("direction",),
                primary_development_r=primary_development_r,
            ),
            "by_symbol_family_direction": _grouped_summaries(
                joined,
                key_fields=("symbol", "setup_family", "direction"),
                primary_development_r=primary_development_r,
            ),
            "by_entry_model": _grouped_summaries(
                joined,
                key_fields=("entry_model",),
                primary_development_r=primary_development_r,
            ),
            "by_practical_rr_bucket": _grouped_summaries(
                joined,
                key_fields=("practical_rr_bucket",),
                primary_development_r=primary_development_r,
            ),
            "by_weekly_cot_asof_cohort": _grouped_summaries(
                joined,
                key_fields=("weekly_cot_asof_cohort",),
                primary_development_r=primary_development_r,
            ),
            "by_operational_positioning_asof_cohort": (
                _grouped_summaries(
                    joined,
                    key_fields=(
                        "operational_positioning_asof_cohort",
                    ),
                    primary_development_r=primary_development_r,
                )
            ),
        },
        "records": joined,
    }


__all__ = [
    "DEFAULT_DEVELOPMENT_THRESHOLDS_R",
    "DEFAULT_PRIMARY_DEVELOPMENT_R",
    "ENTRY_MODEL_NAME",
    "OTD_ORR_EVENT_CENSUS_VERSION",
    "compile_event_census",
    "join_event_and_execution_records",
    "measure_event_development",
    "summarize_event_census",
]
