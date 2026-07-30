from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
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
            1,
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

    def test_cli_exposes_explicit_primary_development_threshold(
        self,
    ) -> None:
        args = build_parser().parse_args(
            ["--development-threshold-r", "2.0"]
        )

        self.assertEqual(args.development_threshold_r, 2.0)


if __name__ == "__main__":
    unittest.main()
