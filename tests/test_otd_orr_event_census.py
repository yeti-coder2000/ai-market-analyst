from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
import unittest
from unittest.mock import patch

import pandas as pd

from app.services.ltf_execution_backtest import (
    HistoricalWatchCandidate,
    compile_backtest_report,
    reconstruct_tpo_watch_candidates,
    run_history_backtest,
)
from app.services.otd_orr_event_census import (
    compile_event_census,
    measure_event_development,
)
from scripts.run_ltf_execution_v2_backtest import (
    build_parser,
    render_markdown_report,
)


def _candidate() -> HistoricalWatchCandidate:
    session_open = datetime(2026, 7, 1, 9, 30, tzinfo=UTC)
    activated = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    expires = datetime(2026, 7, 1, 10, 30, tzinfo=UTC)
    payload = {
        "symbol": "EURUSD",
        "direction": "LONG",
        "signal_alignment": "TREND_ALIGNED",
        "session_scope": "TEST_SESSION",
        "open_location": "OPEN_ABOVE_VALUE",
        "open_behavior": "OPEN_TEST_DRIVE",
        "current_open_behavior": "OPEN_TEST_DRIVE_CONFIRMED",
        "value_acceptance_state": "REJECTED_BACK_OUTSIDE_VALUE",
        "value_rejection_confirmed": True,
        "value_edge_tolerance": 0.05,
        "tpo_watch_active": True,
        "tpo_watch_state": "LTF_MODEL_PENDING",
        "profile_reliability": "UNIT_TEST_RECONSTRUCTED",
        "synthetic_open_confirmed": True,
    }
    return HistoricalWatchCandidate(
        candidate_id="event-1",
        symbol="EURUSD",
        session_id="EURUSD_2026-07-01_TEST",
        reference_profile_id="EURUSD_2026-06-30_TEST",
        setup_family="OPEN_TEST_DRIVE",
        direction="LONG",
        session_open_utc=session_open,
        activated_at_utc=activated,
        expires_at_utc=expires,
        open_price=100.5,
        previous_vah=99.5,
        previous_val=98.0,
        previous_poc=98.8,
        previous_high=100.0,
        previous_low=97.5,
        test_extreme=99.0,
        htf_bias="LONG",
        interest_zones=(),
        payload=payload,
    )


def _history(
    *,
    first_forward_high: float = 100.4,
    first_forward_low: float = 99.6,
    activation_high: float = 100.2,
) -> pd.DataFrame:
    index = pd.date_range(
        "2026-07-01T09:30:00Z",
        periods=12,
        freq="5min",
    )
    frame = pd.DataFrame(
        {
            "open": [100.0] * len(index),
            "high": [100.2] * len(index),
            "low": [99.6] * len(index),
            "close": [100.0] * len(index),
            "volume": [1000.0] * len(index),
        },
        index=index,
    )
    # The 09:55 bar closes at the 10:00 confirmation timestamp.
    frame.loc[pd.Timestamp("2026-07-01T09:55:00Z"), "high"] = (
        activation_high
    )
    # The first post-confirmation bar closes at 10:05.
    frame.loc[pd.Timestamp("2026-07-01T10:00:00Z"), "high"] = (
        first_forward_high
    )
    frame.loc[pd.Timestamp("2026-07-01T10:00:00Z"), "low"] = (
        first_forward_low
    )
    return frame


def _orr_candidate() -> HistoricalWatchCandidate:
    candidate = _candidate()
    payload = {
        **candidate.payload,
        "direction": "SHORT",
        "signal_alignment": "TREND_ALIGNED",
        "open_behavior": "OPEN_REJECTION_REVERSE",
        "current_open_behavior": "OPEN_REJECTION_REVERSE",
        "value_acceptance_state": "REJECTED_BACK_INTO_PRIOR_VALUE",
        "value_rejection_confirmed": False,
    }
    return replace(
        candidate,
        candidate_id="orr-event-1",
        setup_family="OPEN_REJECTION_REVERSE",
        direction="SHORT",
        test_extreme=101.0,
        htf_bias="SHORT",
        payload=payload,
    )


def _execution_row(
    *,
    candidate_id: str = "event-1",
    ready: bool = False,
    outcome: str = "NO_ENTRY_READY",
    filled_at_utc: str | None = None,
    ready_evaluable: bool = True,
    fill_evaluable: bool = True,
    trade_outcome_evaluable: bool = True,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "symbol": "EURUSD",
        "setup_family": "OPEN_TEST_DRIVE",
        "direction": "LONG",
        "activated_at_utc": "2026-07-01T10:00:00+00:00",
        "expires_at_utc": "2026-07-01T10:30:00+00:00",
        "ready": ready,
        "ready_at_utc": (
            "2026-07-01T10:10:00+00:00" if ready else None
        ),
        "filled_at_utc": filled_at_utc,
        "outcome": outcome,
        "gross_R": 2.0 if filled_at_utc else 0.0,
        "net_R": 1.8 if filled_at_utc else 0.0,
        "total_cost_R": 0.2 if filled_at_utc else 0.0,
        "practical_rr_bucket": "2_TO_2_49" if ready else "NOT_READY",
        "retest_depth_ATR": 0.4 if ready else None,
        "stop_distance_ATR": 1.1 if ready else None,
        "execution_cost_model": {
            "source": "UNIT_TEST_COSTS",
        },
        "execution_m5_integrity_status": (
            "COMPLETE_TO_CAUSAL_OUTCOME"
        ),
        "ready_evaluable": ready_evaluable,
        "fill_evaluable": fill_evaluable,
        "trade_outcome_evaluable": trade_outcome_evaluable,
    }


def _reconstruction_history() -> pd.DataFrame:
    friday_index = pd.date_range(
        "2026-07-03T07:00:00Z",
        periods=108,
        freq="5min",
    )
    friday = pd.DataFrame(
        {
            "open": [100.0] * len(friday_index),
            "high": [100.1] * len(friday_index),
            "low": [99.9] * len(friday_index),
            "close": [100.0] * len(friday_index),
            "volume": [1000.0] * len(friday_index),
        },
        index=friday_index,
    )
    monday_index = pd.date_range(
        "2026-07-06T07:00:00Z",
        periods=24,
        freq="5min",
    )
    monday = pd.DataFrame(
        {
            "open": [101.0] * len(monday_index),
            "high": [101.1] * len(monday_index),
            "low": [100.8] * len(monday_index),
            "close": [101.0] * len(monday_index),
            "volume": [1000.0] * len(monday_index),
        },
        index=monday_index,
    )
    monday.iloc[4] = [100.8, 100.9, 99.95, 100.2, 1000.0]
    monday.iloc[5] = [100.2, 100.9, 100.1, 100.8, 1000.0]
    return pd.concat([friday, monday])


class OtdOrrEventCensusTest(unittest.TestCase):
    def test_requested_zero_event_baseline_is_distinct_from_not_provided(
        self,
    ) -> None:
        census = compile_event_census(
            event_records=[],
            execution_rows=[],
        )
        report_without_census_input = compile_backtest_report(
            candidates=[],
            rows=[],
            coverage=[],
        )

        self.assertEqual(census["status"], "OK")
        self.assertEqual(census["metrics"]["all"]["event_count"], 0)
        self.assertEqual(
            report_without_census_input["event_census"]["status"],
            "NOT_PROVIDED",
        )

    def test_event_development_is_measured_without_an_entry_fill(self) -> None:
        event = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
        )
        report = compile_event_census(
            event_records=[event],
            execution_rows=[_execution_row()],
        )

        self.assertTrue(event["event_evaluable"])
        self.assertTrue(event["primary_development_reached"])
        self.assertEqual(
            event["primary_developed_at_utc"],
            "2026-07-01T10:05:00+00:00",
        )
        self.assertEqual(
            event["minutes_confirmation_to_primary_development"],
            5.0,
        )
        self.assertEqual(report["metrics"]["all"]["developed_count"], 1)
        self.assertEqual(
            report["metrics"]["all"]["execution"]["filled_count"],
            0,
        )
        self.assertEqual(
            report["metrics"]["all"]["development_vs_execution"][
                "developed_without_fill_count"
            ],
            1,
        )

    def test_execution_censoring_is_excluded_from_rate_denominators(
        self,
    ) -> None:
        event = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
        )
        pre_ready_censored = _execution_row(
            outcome="NOT_EVALUABLE_RIGHT_CENSORED_BEFORE_ENTRY_READY",
            ready_evaluable=False,
            fill_evaluable=False,
            trade_outcome_evaluable=False,
        )
        pre_ready_censored["execution_m5_integrity_status"] = (
            "RIGHT_CENSORED_BEFORE_ENTRY_READY"
        )

        report = compile_event_census(
            event_records=[event],
            execution_rows=[pre_ready_censored],
        )
        execution = report["metrics"]["all"]["execution"]

        self.assertEqual(execution["eligible_event_count"], 1)
        self.assertEqual(execution["ready_evaluable_event_count"], 0)
        self.assertIsNone(execution["ready_rate"])
        self.assertEqual(
            execution["m5_integrity_status_counts"],
            {"RIGHT_CENSORED_BEFORE_ENTRY_READY": 1},
        )

        ready_censored = _execution_row(
            ready=True,
            outcome="NOT_EVALUABLE_RIGHT_CENSORED_BEFORE_ENTRY_WINDOW",
            ready_evaluable=True,
            fill_evaluable=False,
            trade_outcome_evaluable=False,
        )
        ready_censored["execution_m5_integrity_status"] = (
            "RIGHT_CENSORED_BEFORE_ENTRY_WINDOW"
        )
        report = compile_event_census(
            event_records=[event],
            execution_rows=[ready_censored],
        )
        execution = report["metrics"]["all"]["execution"]

        self.assertEqual(execution["ready_rate"], 1.0)
        self.assertEqual(execution["fill_evaluable_ready_count"], 0)
        self.assertIsNone(execution["fill_rate_of_ready"])

    def test_unconfirmed_synthetic_open_is_excluded_fail_closed(
        self,
    ) -> None:
        candidate = _candidate()
        candidate = replace(
            candidate,
            payload={
                **candidate.payload,
                "synthetic_open_confirmed": False,
                "event_census_execution_eligible": True,
                "event_census_execution_exclusion_reason": None,
            },
        )

        event = measure_event_development(
            candidate,
            _history(first_forward_high=101.6),
        )
        report = compile_event_census(
            event_records=[event],
            execution_rows=[],
        )
        metrics = report["metrics"]["all"]

        self.assertFalse(event["event_evaluable"])
        self.assertFalse(event["execution_universe_eligible"])
        self.assertEqual(
            event["event_evaluation_status"],
            "UNCONFIRMED_SYNTHETIC_OPEN",
        )
        self.assertEqual(
            event["execution_universe_exclusion_reason"],
            "UNCONFIRMED_SYNTHETIC_OPEN",
        )
        self.assertEqual(metrics["development_denominator"], 0)
        self.assertEqual(metrics["execution"]["eligible_event_count"], 0)

    def test_same_bar_development_and_invalidation_is_ambiguous(self) -> None:
        event = measure_event_development(
            _candidate(),
            _history(
                first_forward_high=101.6,
                first_forward_low=98.9,
            ),
        )
        report = compile_event_census(
            event_records=[event],
            execution_rows=[_execution_row()],
        )

        self.assertFalse(event["primary_development_reached"])
        self.assertTrue(event["primary_development_ambiguous"])
        self.assertEqual(
            event["event_outcome"],
            "AMBIGUOUS_DEVELOPMENT_AND_TERMINAL_SAME_BAR",
        )
        self.assertEqual(
            report["metrics"]["all"]["development_denominator"],
            0,
        )
        self.assertIsNone(
            report["metrics"]["all"]["development_rate"]
        )
        self.assertEqual(event["event_mfe_R"], 0.0)
        self.assertEqual(event["event_mae_R"], 0.0)
        self.assertEqual(
            report["metrics"]["all"]["median_event_MFE_R"],
            0.0,
        )
        self.assertEqual(
            report["metrics"]["all"]["median_event_MAE_R"],
            0.0,
        )

    def test_gap_before_development_is_not_evaluable(self) -> None:
        history = _history(first_forward_high=100.4)
        history = history.drop(
            index=pd.Timestamp("2026-07-01T10:00:00Z")
        )
        history.loc[
            pd.Timestamp("2026-07-01T10:05:00Z"),
            "high",
        ] = 101.6

        event = measure_event_development(_candidate(), history)
        report = compile_event_census(
            event_records=[event],
            execution_rows=[_execution_row()],
        )

        self.assertFalse(event["event_evaluable"])
        self.assertFalse(event["primary_development_reached"])
        self.assertEqual(
            event["event_evaluation_status"],
            "INCOMPLETE_FORWARD_M5_SEQUENCE",
        )
        self.assertEqual(
            event["event_outcome"],
            "NOT_EVALUABLE_INCOMPLETE_FORWARD_M5_SEQUENCE",
        )
        self.assertEqual(
            report["metrics"]["all"]["development_denominator"],
            0,
        )
        self.assertEqual(
            report["metrics"]["all"]["data_reliability"][
                "forward_m5_integrity_status_counts"
            ],
            {"INCOMPLETE_FORWARD_M5_SEQUENCE": 1},
        )

    def test_duplicate_forward_bar_cannot_confirm_development(self) -> None:
        history = _history(first_forward_high=101.6)
        history = pd.concat(
            [
                history,
                history.loc[
                    [pd.Timestamp("2026-07-01T10:00:00Z")]
                ],
            ]
        )

        event = measure_event_development(_candidate(), history)
        report = compile_event_census(
            event_records=[event],
            execution_rows=[_execution_row()],
        )

        self.assertFalse(event["event_evaluable"])
        self.assertFalse(event["primary_development_reached"])
        self.assertEqual(
            event["event_evaluation_status"],
            "DUPLICATE_FORWARD_M5_BAR",
        )
        self.assertEqual(
            report["metrics"]["all"]["development_denominator"],
            0,
        )

    def test_truncated_history_is_right_censored_not_failed(self) -> None:
        history = _history(first_forward_high=100.4).loc[
            :"2026-07-01T10:10:00Z"
        ]

        event = measure_event_development(_candidate(), history)
        report = compile_event_census(
            event_records=[event],
            execution_rows=[_execution_row()],
        )
        metrics = report["metrics"]["all"]

        self.assertFalse(event["event_evaluable"])
        self.assertFalse(event["primary_development_reached"])
        self.assertEqual(
            event["event_evaluation_status"],
            "RIGHT_CENSORED_BEFORE_SESSION_HORIZON",
        )
        self.assertEqual(
            event["event_outcome"],
            "NOT_EVALUABLE_RIGHT_CENSORED",
        )
        self.assertEqual(metrics["development_denominator"], 0)
        self.assertEqual(metrics["development_failure_count"], 0)
        self.assertEqual(
            metrics["data_reliability"][
                "forward_m5_integrity_status_counts"
            ],
            {"RIGHT_CENSORED_BEFORE_SESSION_HORIZON": 1},
        )

    def test_gap_after_development_preserves_proven_outcome(self) -> None:
        history = _history(first_forward_high=101.6).drop(
            index=pd.Timestamp("2026-07-01T10:05:00Z")
        )

        event = measure_event_development(_candidate(), history)
        report = compile_event_census(
            event_records=[event],
            execution_rows=[_execution_row()],
        )
        metrics = report["metrics"]["all"]

        self.assertTrue(event["event_evaluable"])
        self.assertTrue(event["primary_development_reached"])
        self.assertEqual(event["event_outcome"], "DEVELOPED")
        self.assertEqual(
            event["primary_developed_at_utc"],
            "2026-07-01T10:05:00+00:00",
        )
        self.assertEqual(
            event["forward_m5_integrity_status"],
            "INCOMPLETE_FORWARD_M5_SEQUENCE",
        )
        self.assertFalse(event["excursion_observation_complete"])
        self.assertEqual(metrics["development_denominator"], 1)
        self.assertEqual(metrics["developed_count"], 1)
        self.assertEqual(
            metrics["thresholds"]["1.50"]["eligible_count"],
            1,
        )
        self.assertEqual(
            metrics["thresholds"]["1.50"]["reached_count"],
            1,
        )
        self.assertEqual(
            metrics["thresholds"]["2.00"]["eligible_count"],
            0,
        )
        self.assertIsNone(metrics["median_event_MFE_R"])
        self.assertIsNone(metrics["median_event_MAE_R"])

    def test_duplicate_after_development_preserves_proven_outcome(
        self,
    ) -> None:
        history = _history(first_forward_high=101.6)
        history = pd.concat(
            [
                history,
                history.loc[
                    [pd.Timestamp("2026-07-01T10:05:00Z")]
                ],
            ]
        )

        event = measure_event_development(_candidate(), history)

        self.assertTrue(event["event_evaluable"])
        self.assertTrue(event["primary_development_reached"])
        self.assertEqual(event["event_outcome"], "DEVELOPED")
        self.assertEqual(
            event["threshold_hits_utc"]["1.50"],
            "2026-07-01T10:05:00+00:00",
        )
        self.assertEqual(
            event["forward_m5_integrity_status"],
            "DUPLICATE_FORWARD_M5_BAR",
        )
        self.assertFalse(event["excursion_observation_complete"])

    def test_right_censoring_after_development_preserves_proven_outcome(
        self,
    ) -> None:
        history = _history(first_forward_high=101.6).loc[
            :"2026-07-01T10:05:00Z"
        ]

        event = measure_event_development(_candidate(), history)

        self.assertTrue(event["event_evaluable"])
        self.assertTrue(event["primary_development_reached"])
        self.assertEqual(event["event_outcome"], "DEVELOPED")
        self.assertEqual(
            event["primary_developed_at_utc"],
            "2026-07-01T10:05:00+00:00",
        )
        self.assertEqual(
            event["forward_m5_integrity_status"],
            "RIGHT_CENSORED_BEFORE_SESSION_HORIZON",
        )
        self.assertFalse(event["excursion_observation_complete"])

    def test_primary_threshold_uses_exact_unique_identity(self) -> None:
        event = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.5),
            primary_development_r=1.504,
        )
        report = compile_event_census(
            event_records=[event],
            execution_rows=[_execution_row()],
            primary_development_r=1.504,
        )

        self.assertFalse(event["primary_development_reached"])
        self.assertIsNotNone(event["threshold_hits_utc"]["1.50"])
        self.assertIsNone(event["threshold_hits_utc"]["1.504"])
        self.assertEqual(
            report["metrics"]["all"]["primary_development_R"],
            1.504,
        )
        self.assertEqual(report["metrics"]["all"]["developed_count"], 0)

    def test_orr_short_development_uses_the_same_causal_event_rules(
        self,
    ) -> None:
        event = measure_event_development(
            _orr_candidate(),
            _history(
                first_forward_high=100.4,
                first_forward_low=98.4,
            ),
        )

        self.assertEqual(event["setup_family"], "OPEN_REJECTION_REVERSE")
        self.assertEqual(event["direction"], "SHORT")
        self.assertTrue(event["primary_development_reached"])
        self.assertEqual(event["structural_invalidation_price"], 101.0)
        self.assertAlmostEqual(event["event_mfe_R"], 1.6)

    def test_confirmation_bar_extreme_cannot_create_future_development(
        self,
    ) -> None:
        event = measure_event_development(
            _candidate(),
            _history(
                activation_high=103.0,
                first_forward_high=100.4,
            ),
        )

        self.assertFalse(event["primary_development_reached"])
        self.assertEqual(event["confirmation_reference_price"], 100.0)
        self.assertLess(event["event_mfe_R"], 1.0)
        self.assertEqual(event["event_outcome"], "EXPIRED_NO_DEVELOPMENT")

    def test_duplicate_confirmation_bar_is_not_evaluable(self) -> None:
        history = _history(first_forward_high=101.6)
        duplicated = pd.concat(
            [
                history,
                history.loc[
                    [pd.Timestamp("2026-07-01T09:55:00Z")]
                ],
            ]
        )
        event = measure_event_development(
            _candidate(),
            duplicated,
        )

        self.assertFalse(event["event_evaluable"])
        self.assertEqual(
            event["event_evaluation_status"],
            "DUPLICATE_CONFIRMATION_BAR",
        )

    def test_census_keeps_development_and_trade_winrate_separate(
        self,
    ) -> None:
        event = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
        )
        execution = _execution_row(
            ready=True,
            outcome="SL_HIT",
            filled_at_utc="2026-07-01T10:15:00+00:00",
        )
        report = compile_event_census(
            event_records=[event],
            execution_rows=[execution],
        )
        metrics = report["metrics"]["all"]

        self.assertEqual(metrics["development_rate"], 1.0)
        self.assertEqual(metrics["execution"]["trade_winrate_closed"], 0.0)
        self.assertEqual(
            metrics["development_vs_execution"][
                "developed_and_sl_count"
            ],
            1,
        )
        self.assertIn(
            "EURUSD|OPEN_TEST_DRIVE|LONG",
            report["metrics"]["by_symbol_family_direction"],
        )

    def test_event_and_execution_candidate_sets_must_match(self) -> None:
        event = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
        )

        with self.assertRaisesRegex(ValueError, "coverage mismatch"):
            compile_event_census(
                event_records=[event],
                execution_rows=[
                    _execution_row(candidate_id="different-candidate")
                ],
            )

    def test_event_execution_join_validates_full_identity(self) -> None:
        event = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
        )
        mismatches = {
            "symbol": "BTCUSD",
            "setup_family": "OPEN_REJECTION_REVERSE",
            "direction": "SHORT",
            "activated_at_utc": "2026-07-01T10:05:00+00:00",
        }

        for field, value in mismatches.items():
            with self.subTest(field=field):
                execution = _execution_row()
                execution[field] = value
                with self.assertRaisesRegex(
                    ValueError,
                    "event/execution identity mismatch",
                ):
                    compile_event_census(
                        event_records=[event],
                        execution_rows=[execution],
                    )

    def test_counter_trend_event_is_counted_but_not_replayed(
        self,
    ) -> None:
        candidate = _candidate()
        candidate = replace(
            candidate,
            payload={
                **candidate.payload,
                "signal_alignment": "COUNTER_TREND",
                "event_census_execution_eligible": False,
                "event_census_execution_exclusion_reason": (
                    "COUNTER_TREND_HARD_GATE"
                ),
            },
        )
        event = measure_event_development(
            candidate,
            _history(first_forward_high=101.6),
        )
        report = compile_event_census(
            event_records=[event],
            execution_rows=[],
        )
        metrics = report["metrics"]["all"]

        self.assertEqual(metrics["developed_count"], 1)
        self.assertEqual(metrics["execution"]["eligible_event_count"], 0)
        self.assertEqual(metrics["execution"]["ready_count"], 0)
        self.assertEqual(
            metrics["execution"]["exclusion_reasons"],
            {"COUNTER_TREND_HARD_GATE": 1},
        )
        self.assertEqual(
            metrics["data_reliability"][
                "expected_execution_exclusion_count"
            ],
            1,
        )
        self.assertFalse(
            report["integrity"][
                "counter_trend_events_can_receive_execution"
            ]
        )

    def test_execution_row_for_hard_gated_event_is_rejected(self) -> None:
        candidate = _candidate()
        candidate = replace(
            candidate,
            payload={
                **candidate.payload,
                "signal_alignment": "COUNTER_TREND",
                "event_census_execution_eligible": False,
                "event_census_execution_exclusion_reason": (
                    "COUNTER_TREND_HARD_GATE"
                ),
            },
        )
        event = measure_event_development(
            candidate,
            _history(first_forward_high=101.6),
        )

        with self.assertRaisesRegex(
            ValueError,
            "execution-ineligible event",
        ):
            compile_event_census(
                event_records=[event],
                execution_rows=[
                    _execution_row(
                        ready=True,
                        outcome="TP_HIT",
                        filled_at_utc="2026-07-01T10:15:00+00:00",
                    )
                ],
            )

    def test_counter_trend_semantics_override_contradictory_eligible_flag(
        self,
    ) -> None:
        candidate = _candidate()
        candidate = replace(
            candidate,
            payload={
                **candidate.payload,
                "signal_alignment": "COUNTER_TREND",
                "event_census_execution_eligible": True,
                "event_census_execution_exclusion_reason": None,
            },
        )
        event = measure_event_development(
            candidate,
            _history(first_forward_high=101.6),
        )

        with self.assertRaisesRegex(
            ValueError,
            "COUNTER_TREND.*execution-ineligible",
        ):
            compile_event_census(
                event_records=[event],
                execution_rows=[
                    _execution_row(
                        ready=True,
                        outcome="TP_HIT",
                        filled_at_utc="2026-07-01T10:15:00+00:00",
                    )
                ],
            )

    def test_filled_unknown_outcomes_are_not_counted_as_known_failures(
        self,
    ) -> None:
        right_censored = measure_event_development(
            _candidate(),
            _history(first_forward_high=100.4).loc[
                :"2026-07-01T10:10:00Z"
            ],
        )
        ambiguous_candidate = replace(
            _candidate(),
            candidate_id="event-2",
        )
        ambiguous = measure_event_development(
            ambiguous_candidate,
            _history(
                first_forward_high=101.6,
                first_forward_low=98.9,
            ),
        )
        report = compile_event_census(
            event_records=[right_censored, ambiguous],
            execution_rows=[
                _execution_row(
                    candidate_id="event-1",
                    ready=True,
                    outcome="SL_HIT",
                    filled_at_utc="2026-07-01T10:15:00+00:00",
                ),
                _execution_row(
                    candidate_id="event-2",
                    ready=True,
                    outcome="SL_HIT",
                    filled_at_utc="2026-07-01T10:15:00+00:00",
                ),
            ],
        )
        comparison = report["metrics"]["all"][
            "development_vs_execution"
        ]

        self.assertEqual(
            comparison["filled_without_primary_development_count"],
            0,
        )
        self.assertEqual(
            comparison[
                "filled_with_ambiguous_primary_development_count"
            ],
            1,
        )
        self.assertEqual(
            comparison["filled_with_not_evaluable_event_count"],
            1,
        )

    def test_compile_rejects_primary_threshold_mismatch(self) -> None:
        event = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
            primary_development_r=1.5,
        )

        with self.assertRaisesRegex(
            ValueError,
            "primary development threshold mismatch",
        ):
            compile_event_census(
                event_records=[event],
                execution_rows=[_execution_row()],
                primary_development_r=2.0,
            )

    def test_compile_does_not_merge_nearby_distinct_thresholds(
        self,
    ) -> None:
        event = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
            primary_development_r=1.5,
        )

        with self.assertRaisesRegex(
            ValueError,
            "primary development threshold mismatch",
        ):
            compile_event_census(
                event_records=[event],
                execution_rows=[_execution_row()],
                primary_development_r=1.5000000005,
            )

    def test_unmeasured_threshold_is_not_counted_as_failure(self) -> None:
        three_r = measure_event_development(
            _candidate(),
            _history(first_forward_high=103.1),
            thresholds_r=(1.0, 1.5, 2.0, 3.0),
        )
        two_r_candidate = replace(
            _candidate(),
            candidate_id="event-2",
        )
        two_r = measure_event_development(
            two_r_candidate,
            _history(first_forward_high=101.6),
            thresholds_r=(1.0, 1.5, 2.0),
        )
        report = compile_event_census(
            event_records=[three_r, two_r],
            execution_rows=[
                _execution_row(candidate_id="event-1"),
                _execution_row(candidate_id="event-2"),
            ],
        )
        three_r_metrics = report["metrics"]["all"]["thresholds"]["3.00"]

        self.assertEqual(three_r_metrics["reached_count"], 1)
        self.assertEqual(three_r_metrics["eligible_count"], 1)
        self.assertEqual(three_r_metrics["not_measured_count"], 1)
        self.assertEqual(three_r_metrics["development_rate"], 1.0)
        self.assertEqual(
            report["definition"]["thresholds_R"],
            [1.0, 1.5, 2.0, 3.0],
        )
        self.assertEqual(
            report["definition"]["threshold_schema_policy"],
            "UNMEASURED_THRESHOLD_EXCLUDED_FROM_DENOMINATOR",
        )

    def test_counter_trend_reconstruction_is_opt_in_for_event_census(
        self,
    ) -> None:
        history = _reconstruction_history()
        with patch(
            "app.services.ltf_execution_backtest._htf_bias",
            return_value="SHORT",
        ):
            execution_candidates, execution_audit = (
                reconstruct_tpo_watch_candidates(
                    history,
                    symbol="EURUSD",
                )
            )
            event_candidates, event_audit = (
                reconstruct_tpo_watch_candidates(
                    history,
                    symbol="EURUSD",
                    include_counter_htf_events=True,
                )
            )

        self.assertEqual(execution_candidates, [])
        self.assertEqual(
            execution_audit["diagnostics"]["skipped_counter_htf"],
            1,
        )
        self.assertEqual(len(event_candidates), 1)
        self.assertEqual(
            event_candidates[0].payload["signal_alignment"],
            "COUNTER_TREND",
        )
        self.assertFalse(
            event_candidates[0].payload[
                "event_census_execution_eligible"
            ]
        )
        self.assertEqual(
            event_audit["diagnostics"][
                "included_counter_htf_event_census"
            ],
            1,
        )

    def test_missing_exact_session_open_bar_cannot_shift_the_open(
        self,
    ) -> None:
        history = _reconstruction_history().drop(
            pd.Timestamp("2026-07-06T07:00:00Z")
        )
        tail = history.loc[
            [pd.Timestamp("2026-07-06T08:55:00Z")]
        ].copy()
        tail.index = pd.DatetimeIndex(
            [pd.Timestamp("2026-07-06T09:00:00Z")]
        )
        history = pd.concat([history, tail])

        candidates, audit = reconstruct_tpo_watch_candidates(
            history,
            symbol="EURUSD",
        )

        monday_candidates = [
            candidate
            for candidate in candidates
            if candidate.session_id.startswith("EURUSD_2026-07-06")
        ]
        self.assertEqual(monday_candidates, [])
        self.assertEqual(
            audit["diagnostics"].get(
                "skipped_missing_exact_session_open_bar"
            ),
            1,
        )

    def test_gap_between_open_and_confirmation_rejects_candidate(
        self,
    ) -> None:
        history = _reconstruction_history().drop(
            pd.Timestamp("2026-07-06T07:10:00Z")
        )
        tail = history.loc[
            [pd.Timestamp("2026-07-06T08:55:00Z")]
        ].copy()
        tail.index = pd.DatetimeIndex(
            [pd.Timestamp("2026-07-06T09:00:00Z")]
        )
        history = pd.concat([history, tail])

        candidates, audit = reconstruct_tpo_watch_candidates(
            history,
            symbol="EURUSD",
        )

        monday_candidates = [
            candidate
            for candidate in candidates
            if candidate.session_id.startswith("EURUSD_2026-07-06")
        ]
        self.assertEqual(monday_candidates, [])
        self.assertEqual(
            audit["diagnostics"].get(
                "skipped_incomplete_open_to_confirmation_m5"
            ),
            1,
        )

    def test_backtest_report_embeds_chronological_event_census(self) -> None:
        candidate = _candidate()
        event = measure_event_development(
            candidate,
            _history(first_forward_high=101.6),
        )
        execution = _execution_row()
        report = compile_backtest_report(
            candidates=[candidate],
            rows=[execution],
            event_records=[event],
            coverage=[
                {
                    "symbol": "EURUSD",
                    "history_first_bar_utc": (
                        "2026-07-01T09:30:00+00:00"
                    ),
                    "history_last_bar_utc": (
                        "2026-07-01T10:25:00+00:00"
                    ),
                }
            ],
        )

        self.assertEqual(report["event_census"]["status"], "OK")
        self.assertTrue(
            report["execution_integrity"][
                "development_and_trade_metrics_are_separate"
            ]
        )
        self.assertFalse(
            report["event_census"]["integrity"]["look_ahead_allowed"]
        )
        self.assertEqual(event["weekly_cot_asof_cohort"], "NO_DATA")
        self.assertEqual(
            report["event_census"]["integrity"][
                "weekly_cot_event_join"
            ],
            "NO_DATA_UNTIL_CAUSAL_PUBLICATION_TIMESTAMPS_EXIST",
        )
        markdown = render_markdown_report(report)
        self.assertIn("Backtest Integrity v2.0", markdown)
        self.assertIn("EURUSD | OPEN_TEST_DRIVE | LONG", markdown)
        self.assertIn(
            "Development rate and trade win rate use separate denominators",
            markdown,
        )

    def test_history_backtest_builds_event_and_execution_universes(
        self,
    ) -> None:
        with patch(
            "app.services.ltf_execution_backtest._htf_bias",
            return_value="LONG",
        ):
            report = run_history_backtest(
                {"EURUSD": _reconstruction_history()}
            )

        self.assertEqual(report["event_census"]["status"], "OK")
        self.assertEqual(
            report["event_census"]["metrics"]["all"]["event_count"],
            1,
        )
        self.assertEqual(
            report["coverage"][0]["event_candidate_count"],
            1,
        )
        self.assertEqual(
            report["coverage"][0]["execution_candidate_count"],
            0,
        )
        self.assertEqual(
            report["event_census"]["metrics"]["all"][
                "development_denominator"
            ],
            0,
        )
        self.assertEqual(
            report["event_census"]["records"][0][
                "event_evaluation_status"
            ],
            "UNCONFIRMED_SYNTHETIC_OPEN",
        )

    def test_event_holdout_is_chronological_across_full_event_universe(
        self,
    ) -> None:
        first = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
        )
        second = {
            **first,
            "candidate_id": "event-2",
            "session_id": "EURUSD_2026-07-08_TEST",
            "session_open_utc": "2026-07-08T09:30:00+00:00",
            "confirmed_at_utc": "2026-07-08T10:00:00+00:00",
            "expires_at_utc": "2026-07-08T10:30:00+00:00",
            "primary_developed_at_utc": (
                "2026-07-08T10:05:00+00:00"
            ),
            "execution_universe_eligible": False,
            "execution_universe_exclusion_reason": (
                "COUNTER_TREND_HARD_GATE"
            ),
        }
        report = compile_event_census(
            event_records=[second, first],
            execution_rows=[_execution_row()],
            holdout_fraction=0.50,
        )

        self.assertEqual(report["split_index"], 1)
        self.assertEqual(
            report["metrics"]["development"]["event_count"],
            1,
        )
        self.assertEqual(report["metrics"]["holdout"]["event_count"], 1)
        self.assertEqual(
            report["metrics"]["holdout"]["execution"][
                "excluded_event_count"
            ],
            1,
        )

    def test_execution_uses_full_event_universe_holdout_cutoff(
        self,
    ) -> None:
        base_candidate = _candidate()
        base_event = measure_event_development(
            base_candidate,
            _history(first_forward_high=101.6),
        )
        events: list[dict[str, object]] = []
        candidates: list[HistoricalWatchCandidate] = []
        execution_rows: list[dict[str, object]] = []

        for offset in range(10):
            confirmed = datetime(
                2026,
                7,
                1,
                10,
                0,
                tzinfo=UTC,
            ) + timedelta(days=offset)
            candidate_id = f"event-{offset + 1}"
            execution_eligible = offset % 2 == 1
            event = {
                **base_event,
                "candidate_id": candidate_id,
                "session_id": f"EURUSD_2026-07-{offset + 1:02d}_TEST",
                "session_open_utc": (
                    confirmed - timedelta(minutes=30)
                ).isoformat(),
                "confirmed_at_utc": confirmed.isoformat(),
                "expires_at_utc": (
                    confirmed + timedelta(minutes=30)
                ).isoformat(),
                "execution_universe_eligible": execution_eligible,
                "execution_universe_exclusion_reason": (
                    None
                    if execution_eligible
                    else "COUNTER_TREND_HARD_GATE"
                ),
                "htf_alignment_state": (
                    "TREND_ALIGNED"
                    if execution_eligible
                    else "COUNTER_TREND"
                ),
            }
            events.append(event)
            if not execution_eligible:
                continue

            candidate = replace(
                base_candidate,
                candidate_id=candidate_id,
                session_id=str(event["session_id"]),
                session_open_utc=confirmed - timedelta(minutes=30),
                activated_at_utc=confirmed,
                expires_at_utc=confirmed + timedelta(minutes=30),
            )
            candidates.append(candidate)
            execution = _execution_row(
                candidate_id=candidate_id,
                ready=True,
                outcome="TP_HIT",
                filled_at_utc=(
                    confirmed + timedelta(minutes=15)
                ).isoformat(),
            )
            execution["activated_at_utc"] = confirmed.isoformat()
            execution["expires_at_utc"] = (
                confirmed + timedelta(minutes=30)
            ).isoformat()
            execution_rows.append(execution)

        report = compile_backtest_report(
            candidates=candidates,
            rows=execution_rows,
            event_records=events,
            coverage=[
                {
                    "symbol": "EURUSD",
                    "history_first_bar_utc": (
                        "2026-07-01T00:00:00+00:00"
                    ),
                    "history_last_bar_utc": (
                        "2026-07-10T23:55:00+00:00"
                    ),
                    "history_last_bar_close_utc": (
                        "2026-07-11T00:00:00+00:00"
                    ),
                }
            ],
            holdout_fraction=0.30,
        )

        expected_cutoff = "2026-07-08T10:00:00+00:00"
        self.assertEqual(report["holdout_start_utc"], expected_cutoff)
        self.assertEqual(
            report["event_census"]["holdout_start_utc"],
            expected_cutoff,
        )
        self.assertEqual(report["event_census"]["split_index"], 7)
        self.assertEqual(report["split_index"], 3)
        self.assertEqual(
            report["event_census"]["metrics"]["holdout"]["event_count"],
            3,
        )
        self.assertEqual(
            report["metrics"]["holdout"]["candidate_count"],
            2,
        )
        development = report["metrics"]["development"]
        holdout = report["metrics"]["holdout"]
        self.assertEqual(
            development["window_start_utc"],
            "2026-07-01T00:00:00+00:00",
        )
        self.assertEqual(development["window_end_utc"], expected_cutoff)
        self.assertEqual(holdout["window_start_utc"], expected_cutoff)
        self.assertEqual(
            holdout["window_end_utc"],
            "2026-07-11T00:00:00+00:00",
        )
        development_weeks = (
            datetime(2026, 7, 8, 10, 0, tzinfo=UTC)
            - datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
        ).total_seconds() / 604800.0
        holdout_weeks = (
            datetime(2026, 7, 11, 0, 0, tzinfo=UTC)
            - datetime(2026, 7, 8, 10, 0, tzinfo=UTC)
        ).total_seconds() / 604800.0
        self.assertAlmostEqual(
            development["ready_signals_per_week"],
            development["ready_count"] / development_weeks,
        )
        self.assertAlmostEqual(
            development["filled_signals_per_week"],
            development["filled_count"] / development_weeks,
        )
        self.assertAlmostEqual(
            holdout["ready_signals_per_week"],
            holdout["ready_count"] / holdout_weeks,
        )
        self.assertAlmostEqual(
            holdout["filled_signals_per_week"],
            holdout["filled_count"] / holdout_weeks,
        )

    def test_headline_metrics_use_common_asset_coverage_overlap(
        self,
    ) -> None:
        base_event = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
        )
        definitions = [
            (
                "eur-old",
                "EURUSD",
                datetime(2025, 7, 1, 10, 0, tzinfo=UTC),
            ),
            (
                "eur-overlap",
                "EURUSD",
                datetime(2026, 7, 2, 10, 0, tzinfo=UTC),
            ),
            (
                "ger-overlap",
                "GER40",
                datetime(2026, 7, 8, 10, 0, tzinfo=UTC),
            ),
        ]
        events = []
        execution_rows = []
        for candidate_id, symbol, confirmed in definitions:
            event = {
                **base_event,
                "candidate_id": candidate_id,
                "symbol": symbol,
                "session_id": f"{symbol}_{confirmed.date()}_TEST",
                "session_open_utc": (
                    confirmed - timedelta(minutes=30)
                ).isoformat(),
                "confirmed_at_utc": confirmed.isoformat(),
                "expires_at_utc": (
                    confirmed + timedelta(minutes=30)
                ).isoformat(),
            }
            execution = _execution_row(
                candidate_id=candidate_id,
                ready=True,
                outcome="TP_HIT",
                filled_at_utc=(
                    confirmed + timedelta(minutes=15)
                ).isoformat(),
            )
            execution.update(
                {
                    "symbol": symbol,
                    "activated_at_utc": confirmed.isoformat(),
                    "expires_at_utc": (
                        confirmed + timedelta(minutes=30)
                    ).isoformat(),
                }
            )
            events.append(event)
            execution_rows.append(execution)

        report = compile_backtest_report(
            candidates=[],
            rows=execution_rows,
            event_records=events,
            coverage=[
                {
                    "symbol": "EURUSD",
                    "history_first_bar_utc": (
                        "2024-07-01T00:00:00+00:00"
                    ),
                    "history_last_bar_close_utc": (
                        "2026-07-11T00:00:00+00:00"
                    ),
                },
                {
                    "symbol": "GER40",
                    "history_first_bar_utc": (
                        "2026-07-01T00:00:00+00:00"
                    ),
                    "history_last_bar_close_utc": (
                        "2026-07-10T00:00:00+00:00"
                    ),
                },
            ],
            holdout_fraction=0.50,
        )

        self.assertEqual(
            report["headline_coverage"]["status"],
            "AVAILABLE_COMMON_ASSET_OVERLAP",
        )
        self.assertEqual(
            report["headline_coverage"]["window_start_utc"],
            "2026-07-01T00:00:00+00:00",
        )
        self.assertEqual(
            report["headline_coverage"]["window_end_utc"],
            "2026-07-10T00:00:00+00:00",
        )
        self.assertEqual(
            report["headline_coverage"][
                "excluded_execution_candidate_count_outside_overlap"
            ],
            1,
        )
        self.assertEqual(report["metrics"]["all"]["candidate_count"], 2)
        self.assertEqual(
            report["event_census"]["metrics"]["all"]["event_count"],
            2,
        )
        self.assertEqual(len(report["event_census"]["records"]), 3)
        self.assertEqual(
            report["event_census"]["metrics"]["by_symbol"]["EURUSD"][
                "event_count"
            ],
            2,
        )
        self.assertEqual(
            report["metrics"]["by_family"]["OPEN_TEST_DRIVE"][
                "candidate_count"
            ],
            2,
        )
        self.assertEqual(
            report["metrics"]["by_direction"]["LONG"]["candidate_count"],
            2,
        )
        self.assertEqual(
            report["metrics"]["by_symbol"]["EURUSD"]["candidate_count"],
            2,
        )
        eur_window_weeks = (
            datetime(2026, 7, 11, tzinfo=UTC)
            - datetime(2024, 7, 1, tzinfo=UTC)
        ).total_seconds() / 604800.0
        self.assertAlmostEqual(
            report["metrics"]["by_symbol"]["EURUSD"][
                "ready_signals_per_week"
            ],
            2 / eur_window_weeks,
        )
        self.assertEqual(
            report["metrics"]["by_symbol"]["EURUSD"][
                "frequency_coverage_scope"
            ],
            "ASSET_PROVIDER_COVERAGE:EURUSD",
        )
        self.assertEqual(
            report["metrics"]["by_symbol_family"][
                "EURUSD|OPEN_TEST_DRIVE"
            ]["frequency_coverage_scope"],
            "ASSET_PROVIDER_COVERAGE:EURUSD",
        )
        for cohort in (
            report["metrics"]["by_family"]["OPEN_TEST_DRIVE"],
            report["metrics"]["by_direction"]["LONG"],
            report["metrics"]["by_practical_rr_bucket"]["2_TO_2_49"],
        ):
            self.assertEqual(
                cohort["frequency_coverage_scope"],
                "COMMON_ASSET_OVERLAP",
            )
            self.assertEqual(
                cohort["window_start_utc"],
                "2026-07-01T00:00:00+00:00",
            )
            self.assertEqual(
                cohort["window_end_utc"],
                "2026-07-10T00:00:00+00:00",
            )

    def test_unavailable_holdout_is_explicit_in_json_and_markdown(
        self,
    ) -> None:
        event = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
        )
        execution = _execution_row(
            ready=True,
            outcome="TP_HIT",
            filled_at_utc="2026-07-01T10:15:00+00:00",
        )
        report = compile_backtest_report(
            candidates=[_candidate()],
            rows=[execution],
            event_records=[event],
            coverage=[
                {
                    "symbol": "EURUSD",
                    "history_first_bar_utc": (
                        "2026-07-01T00:00:00+00:00"
                    ),
                    "history_last_bar_close_utc": (
                        "2026-07-02T00:00:00+00:00"
                    ),
                }
            ],
            holdout_fraction=0.30,
        )

        self.assertEqual(
            report["holdout_status"],
            "UNAVAILABLE_NO_DISTINCT_TEMPORAL_CUTOFF",
        )
        self.assertEqual(
            report["event_census"]["holdout_status"],
            "UNAVAILABLE_NO_DISTINCT_TEMPORAL_CUTOFF",
        )
        self.assertIsNone(
            report["metrics"]["holdout"]["ready_signals_per_week"]
        )
        self.assertIsNone(
            report["metrics"]["holdout"]["filled_signals_per_week"]
        )

        markdown = render_markdown_report(report)
        self.assertIn(
            "Holdout status: `UNAVAILABLE_NO_DISTINCT_TEMPORAL_CUTOFF`",
            markdown,
        )
        self.assertIn(
            "| Metric | Full | Development | Holdout |",
            markdown,
        )
        self.assertIn("| READY signals/week |", markdown)
        self.assertIn("| Filled signals/week |", markdown)

    def test_cutoff_at_coverage_end_does_not_invent_holdout_frequency(
        self,
    ) -> None:
        first = measure_event_development(
            _candidate(),
            _history(first_forward_high=101.6),
        )
        second = {
            **first,
            "candidate_id": "event-at-coverage-end",
            "session_id": "EURUSD_2026-07-02_TEST",
            "session_open_utc": "2026-07-02T09:30:00+00:00",
            "confirmed_at_utc": "2026-07-02T10:00:00+00:00",
            "expires_at_utc": "2026-07-02T10:30:00+00:00",
        }
        first_execution = _execution_row()
        second_execution = _execution_row(
            candidate_id="event-at-coverage-end",
            ready=True,
            outcome="TP_HIT",
            filled_at_utc="2026-07-02T10:00:00+00:00",
        )
        second_execution.update(
            {
                "activated_at_utc": "2026-07-02T10:00:00+00:00",
                "expires_at_utc": "2026-07-02T10:30:00+00:00",
            }
        )
        report = compile_backtest_report(
            candidates=[],
            rows=[first_execution, second_execution],
            event_records=[first, second],
            coverage=[
                {
                    "symbol": "EURUSD",
                    "history_first_bar_utc": (
                        "2026-07-01T00:00:00+00:00"
                    ),
                    "history_last_bar_utc": (
                        "2026-07-02T09:55:00+00:00"
                    ),
                    "history_last_bar_close_utc": (
                        "2026-07-02T10:00:00+00:00"
                    ),
                }
            ],
            holdout_fraction=0.50,
        )

        self.assertEqual(
            report["holdout_status"],
            "UNAVAILABLE_ZERO_LENGTH_COVERAGE_WINDOW",
        )
        self.assertEqual(
            report["metrics"]["holdout"]["frequency_status"],
            "UNAVAILABLE_ZERO_LENGTH_WINDOW",
        )
        self.assertIsNone(
            report["metrics"]["holdout"]["ready_signals_per_week"]
        )
        self.assertIsNone(
            report["metrics"]["holdout"]["filled_signals_per_week"]
        )

    def test_markdown_renders_actual_holdout_fraction_and_cutoff(
        self,
    ) -> None:
        first = {
            **measure_event_development(
                _candidate(),
                _history(first_forward_high=101.6),
            ),
            "execution_universe_eligible": False,
            "execution_universe_exclusion_reason": (
                "COUNTER_TREND_HARD_GATE"
            ),
            "htf_alignment_state": "COUNTER_TREND",
        }
        second = {
            **first,
            "candidate_id": "event-2",
            "session_id": "EURUSD_2026-07-08_TEST",
            "session_open_utc": "2026-07-08T09:30:00+00:00",
            "confirmed_at_utc": "2026-07-08T10:00:00+00:00",
            "expires_at_utc": "2026-07-08T10:30:00+00:00",
        }
        report = compile_backtest_report(
            candidates=[],
            rows=[],
            event_records=[first, second],
            coverage=[
                {
                    "symbol": "EURUSD",
                    "history_first_bar_utc": (
                        "2026-07-01T00:00:00+00:00"
                    ),
                    "history_last_bar_utc": (
                        "2026-07-08T23:55:00+00:00"
                    ),
                }
            ],
            holdout_fraction=0.50,
        )

        markdown = render_markdown_report(report)

        self.assertIn("Chronological 50% holdout", markdown)
        self.assertIn(
            "Holdout cutoff UTC: `2026-07-08T10:00:00+00:00`",
            markdown,
        )
        self.assertNotIn("Chronological 30% holdout", markdown)

    def test_cli_exposes_explicit_primary_development_threshold(
        self,
    ) -> None:
        args = build_parser().parse_args(
            ["--development-threshold-r", "2.0"]
        )

        self.assertEqual(args.development_threshold_r, 2.0)


if __name__ == "__main__":
    unittest.main()
