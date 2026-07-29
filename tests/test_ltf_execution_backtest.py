from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from app.services.ltf_execution_backtest import (
    ExecutionCostModel,
    HistoricalContextPoint,
    HistoricalWatchCandidate,
    _simulate_limit_outcome,
    build_dynamic_context_timeline,
    compile_backtest_report,
    reconstruct_tpo_watch_candidates,
    replay_candidate,
)
from scripts.run_ltf_execution_v2_backtest import (
    ProviderDepthBacktestError,
    TWELVEDATA_SYMBOLS,
    YFINANCE_SYMBOLS,
    _active_symbols,
    fetch_all_histories,
    fetch_twelvedata_max_history,
    load_execution_cost_models,
    summarize_operational_positioning_history,
)


def _history() -> pd.DataFrame:
    rows = [
        ("2026-07-01T09:20:00Z", 100.00, 100.20, 99.90, 100.10),
        ("2026-07-01T09:25:00Z", 100.10, 100.25, 100.00, 100.15),
        ("2026-07-01T09:30:00Z", 100.15, 100.30, 100.05, 100.20),
        ("2026-07-01T09:35:00Z", 100.20, 100.35, 100.10, 100.25),
        ("2026-07-01T09:40:00Z", 100.25, 100.40, 100.15, 100.30),
        ("2026-07-01T09:45:00Z", 100.30, 100.42, 100.20, 100.28),
        ("2026-07-01T09:50:00Z", 100.28, 100.38, 100.18, 100.25),
        ("2026-07-01T09:55:00Z", 100.25, 100.40, 100.20, 100.30),
        ("2026-07-01T10:00:00Z", 100.30, 101.30, 100.25, 101.20),
        ("2026-07-01T10:05:00Z", 101.20, 101.35, 100.90, 101.05),
        ("2026-07-01T10:10:00Z", 100.60, 100.90, 100.34, 100.80),
        ("2026-07-01T10:15:00Z", 100.78, 101.12, 100.72, 101.05),
        ("2026-07-01T10:20:00Z", 101.05, 101.15, 100.95, 101.08),
        ("2026-07-01T10:25:00Z", 100.80, 100.90, 100.30, 100.70),
        ("2026-07-01T10:30:00Z", 100.70, 103.20, 100.65, 103.00),
    ]
    return pd.DataFrame(
        {
            "open": [row[1] for row in rows],
            "high": [row[2] for row in rows],
            "low": [row[3] for row in rows],
            "close": [row[4] for row in rows],
            "volume": [1000.0] * len(rows),
        },
        index=pd.to_datetime([row[0] for row in rows], utc=True),
    )


def _candidate() -> HistoricalWatchCandidate:
    activated = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    expires = datetime(2026, 7, 1, 11, 0, tzinfo=UTC)
    zones = (
        {
            "zone_type": "NPOC",
            "role": "TARGET",
            "price": 103.0,
            "profile_id": "EURUSD_2026-06-30",
        },
    )
    payload = {
        "symbol": "EURUSD",
        "direction": "LONG",
        "tpo_watch_state": "LTF_MODEL_PENDING",
        "tpo_watch_active": True,
        "tpo_watch_setup": "OPEN_TEST_DRIVE",
        "current_open_behavior": "OPEN_TEST_DRIVE_CONFIRMED",
        "open_behavior": "OPEN_TEST_DRIVE",
        "open_location": "OPEN_ABOVE_VALUE",
        "value_acceptance_state": "REJECTED_BACK_OUTSIDE_VALUE",
        "value_rejection_confirmed": True,
        "value_test_level": "VAH",
        "previous_vah": 100.20,
        "previous_val": 98.50,
        "previous_poc": 99.30,
        "previous_low": 97.80,
        "current_session_id": "EURUSD_2026-07-01",
        "reference_profile_id": "EURUSD_2026-06-30",
        "context_invalidation_price": 99.50,
        "interest_zones": list(zones),
        "expires_at_utc": expires.isoformat(),
    }
    return HistoricalWatchCandidate(
        candidate_id="candidate-1",
        symbol="EURUSD",
        session_id="EURUSD_2026-07-01",
        reference_profile_id="EURUSD_2026-06-30",
        setup_family="OPEN_TEST_DRIVE",
        direction="LONG",
        session_open_utc=activated,
        activated_at_utc=activated,
        expires_at_utc=expires,
        open_price=101.0,
        previous_vah=100.20,
        previous_val=98.50,
        previous_poc=99.30,
        previous_high=101.5,
        previous_low=97.8,
        test_extreme=99.5,
        htf_bias="LONG",
        interest_zones=zones,
        payload=payload,
    )


def _outcome_bars(
    rows: list[tuple[str, float, float, float, float]],
) -> pd.DataFrame:
    opens = pd.to_datetime([row[0] for row in rows], utc=True)
    return pd.DataFrame(
        {
            "bar_open_utc": opens - pd.Timedelta(minutes=5),
            "bar_close_utc": opens,
            "open": [row[1] for row in rows],
            "high": [row[2] for row in rows],
            "low": [row[3] for row in rows],
            "close": [row[4] for row in rows],
            "volume": [1000.0] * len(rows),
        }
    )


def _context_history(*, inside_value: bool) -> pd.DataFrame:
    prehistory_index = pd.date_range(
        "2026-07-01T09:00:00Z",
        periods=12,
        freq="5min",
    )
    context_index = pd.date_range(
        "2026-07-01T10:00:00Z",
        periods=12,
        freq="5min",
    )
    prehistory = pd.DataFrame(
        {
            "open": [101.0] * len(prehistory_index),
            "high": [101.1] * len(prehistory_index),
            "low": [100.9] * len(prehistory_index),
            "close": [101.0] * len(prehistory_index),
            "volume": [1000.0] * len(prehistory_index),
        },
        index=prehistory_index,
    )
    if inside_value:
        context_open = 100.05
        context_high = 100.10
        context_low = 99.90
        context_close = 100.00
    else:
        context_open = 101.00
        context_high = 101.10
        context_low = 100.90
        context_close = 101.00
    context = pd.DataFrame(
        {
            "open": [context_open] * len(context_index),
            "high": [context_high] * len(context_index),
            "low": [context_low] * len(context_index),
            "close": [context_close] * len(context_index),
            "volume": [1000.0] * len(context_index),
        },
        index=context_index,
    )
    return pd.concat([prehistory, context])


class LtfExecutionBacktestTest(unittest.TestCase):
    def test_yfinance_depth_scope_covers_all_approved_symbols(self) -> None:
        self.assertEqual(
            set(YFINANCE_SYMBOLS),
            {"GER40", "NAS100", "SPX500", "UKOIL"},
        )
        self.assertEqual(YFINANCE_SYMBOLS["NAS100"], ("^NDX",))
        self.assertEqual(YFINANCE_SYMBOLS["SPX500"], ("^GSPC",))
        self.assertEqual(YFINANCE_SYMBOLS["UKOIL"], ("BZ=F",))
        self.assertEqual(
            set(_active_symbols()),
            set(TWELVEDATA_SYMBOLS) | set(YFINANCE_SYMBOLS),
        )

    def test_operational_positioning_uses_only_persisted_snapshots(self) -> None:
        snapshots = [
            {
                "generated_at": "2026-07-24T08:00:00+00:00",
                "items": [
                    {
                        "symbol": "BTCUSD",
                        "raw_source": {
                            "open_interest": 1000.0,
                            "funding_rate_pct": 0.01,
                        },
                    },
                    {
                        "symbol": "ETHUSD",
                        "raw_source": {"open_interest": 500.0},
                    },
                ],
            },
            {
                "generated_at": "2026-07-25T08:00:00+00:00",
                "items": [
                    {
                        "symbol": "BTCUSD",
                        "daily_market_data": {
                            "open_interest_change_pct": 2.5,
                        },
                    }
                ],
            },
        ]
        with tempfile.TemporaryDirectory(prefix="positioning-history-") as tmp:
            path = Path(tmp) / "daily_positioning_history.jsonl"
            path.write_text(
                "\n".join(json.dumps(item) for item in snapshots) + "\n",
                encoding="utf-8",
            )

            summary = summarize_operational_positioning_history(path)

        self.assertEqual(summary["status"], "AVAILABLE")
        self.assertEqual(summary["snapshot_count"], 2)
        self.assertEqual(summary["snapshots_with_open_interest"], 2)
        self.assertEqual(summary["snapshots_with_funding"], 1)
        self.assertEqual(
            summary["symbols"]["BTCUSD"]["oi_snapshot_count"],
            2,
        )
        self.assertEqual(
            summary["symbols"]["BTCUSD"]["funding_snapshot_count"],
            1,
        )
        self.assertEqual(
            summary["symbols"]["ETHUSD"]["oi_snapshot_count"],
            1,
        )

    def test_operational_positioning_does_not_invent_missing_history(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="positioning-history-") as tmp:
            missing = Path(tmp) / "missing.jsonl"
            summary = summarize_operational_positioning_history(missing)

        self.assertEqual(summary["status"], "NO_HISTORICAL_SNAPSHOTS")
        self.assertEqual(summary["snapshot_count"], 0)

    def test_replay_is_causal_and_scores_future_limit_fill(self) -> None:
        candidate = _candidate()
        row = replay_candidate(candidate, _history())

        self.assertTrue(row["ready"])
        self.assertEqual(row["ready_at_utc"], "2026-07-01T10:20:00+00:00")
        self.assertEqual(row["filled_at_utc"], "2026-07-01T10:30:00+00:00")
        self.assertEqual(
            row["entry_window_expires_at_utc"],
            "2026-07-01T10:50:00+00:00",
        )
        self.assertLessEqual(
            pd.Timestamp(row["filled_at_utc"]),
            pd.Timestamp(row["entry_window_expires_at_utc"]),
        )
        self.assertGreaterEqual(
            pd.Timestamp(row["resolved_at_utc"]),
            pd.Timestamp(row["filled_at_utc"]),
        )
        self.assertEqual(row["outcome"], "TP_HIT")
        self.assertGreaterEqual(row["realized_R"], 2.0)
        transition_times = [
            pd.Timestamp(item["at_utc"])
            for item in row["transition_history"]
        ]
        self.assertEqual(transition_times, sorted(transition_times))

    def test_fill_after_30_minute_entry_window_is_not_allowed(self) -> None:
        ready_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
        outcome = _simulate_limit_outcome(
            _outcome_bars(
                [
                    (
                        "2026-07-01T10:31:00Z",
                        100.2,
                        100.5,
                        99.5,
                        100.1,
                    )
                ]
            ),
            direction="LONG",
            ready_at=ready_at,
            entry_window_expires_at=ready_at + timedelta(minutes=30),
            trade_resolution_expires_at=ready_at + timedelta(hours=8),
            entry=100.0,
            stop=99.0,
            target=102.0,
        )

        self.assertEqual(outcome["outcome"], "ENTRY_WINDOW_EXPIRED_UNFILLED")
        self.assertIsNone(outcome["filled_at_utc"])
        self.assertEqual(
            outcome["resolved_at_utc"],
            "2026-07-01T10:30:00+00:00",
        )

    def test_invalidation_before_limit_fill_cancels_order(self) -> None:
        ready_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
        outcome = _simulate_limit_outcome(
            _outcome_bars(
                [
                    (
                        "2026-07-01T10:05:00Z",
                        99.3,
                        99.5,
                        98.5,
                        99.0,
                    ),
                    (
                        "2026-07-01T10:10:00Z",
                        99.8,
                        100.5,
                        99.5,
                        100.1,
                    ),
                ]
            ),
            direction="LONG",
            ready_at=ready_at,
            entry_window_expires_at=ready_at + timedelta(minutes=30),
            trade_resolution_expires_at=ready_at + timedelta(hours=8),
            entry=100.0,
            stop=99.0,
            target=102.0,
        )

        self.assertEqual(outcome["outcome"], "INVALIDATED_BEFORE_FILL")
        self.assertIsNone(outcome["filled_at_utc"])
        self.assertEqual(
            outcome["cancellation_reason"],
            "STOP_OR_CONTEXT_INVALIDATION_BEFORE_FILL",
        )

    def test_context_change_after_ready_cancels_unfilled_limit(self) -> None:
        ready_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
        outcome = _simulate_limit_outcome(
            _outcome_bars(
                [
                    (
                        "2026-07-01T10:10:00Z",
                        99.5,
                        99.8,
                        99.3,
                        99.6,
                    ),
                    (
                        "2026-07-01T10:15:00Z",
                        99.8,
                        100.5,
                        99.5,
                        100.1,
                    ),
                ]
            ),
            direction="LONG",
            ready_at=ready_at,
            entry_window_expires_at=ready_at + timedelta(minutes=30),
            trade_resolution_expires_at=ready_at + timedelta(hours=8),
            entry=100.0,
            stop=99.0,
            target=102.0,
            context_timeline=[
                HistoricalContextPoint(
                    as_of_utc=ready_at + timedelta(minutes=10),
                    payload_updates={
                        "tpo_watch_active": False,
                        "current_open_behavior": "OPEN_AUCTION",
                    },
                    cancellation_reason="OTD_TRANSITIONED_TO_OPEN_AUCTION",
                )
            ],
        )

        self.assertEqual(outcome["outcome"], "CONTEXT_CANCELLED_BEFORE_FILL")
        self.assertIsNone(outcome["filled_at_utc"])
        self.assertEqual(
            outcome["cancellation_reason"],
            "OTD_TRANSITIONED_TO_OPEN_AUCTION",
        )

    def test_filled_trade_can_resolve_after_entry_window(self) -> None:
        ready_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
        outcome = _simulate_limit_outcome(
            _outcome_bars(
                [
                    (
                        "2026-07-01T10:25:00Z",
                        100.2,
                        100.5,
                        99.9,
                        100.3,
                    ),
                    (
                        "2026-07-01T10:35:00Z",
                        100.3,
                        101.0,
                        100.2,
                        100.8,
                    ),
                    (
                        "2026-07-01T11:00:00Z",
                        100.8,
                        102.1,
                        100.7,
                        102.0,
                    ),
                ]
            ),
            direction="LONG",
            ready_at=ready_at,
            entry_window_expires_at=ready_at + timedelta(minutes=30),
            trade_resolution_expires_at=ready_at + timedelta(hours=2),
            entry=100.0,
            stop=99.0,
            target=102.0,
        )

        self.assertEqual(outcome["outcome"], "TP_HIT")
        self.assertEqual(
            outcome["filled_at_utc"],
            "2026-07-01T10:25:00+00:00",
        )
        self.assertEqual(
            outcome["resolved_at_utc"],
            "2026-07-01T11:00:00+00:00",
        )

    def test_otd_acceptance_back_inside_value_cancels_before_ready(
        self,
    ) -> None:
        candidate = _candidate()
        timeline = build_dynamic_context_timeline(
            candidate,
            _context_history(inside_value=True),
        )
        first_cancel = next(
            point for point in timeline if point.cancellation_reason
        )
        row = replay_candidate(
            candidate,
            _context_history(inside_value=True),
        )

        self.assertEqual(
            first_cancel.cancellation_reason,
            "OTD_ACCEPTED_BACK_INTO_VALUE",
        )
        self.assertEqual(
            first_cancel.payload_updates["current_open_behavior"],
            "OPEN_REJECTION_REVERSE",
        )
        self.assertFalse(row["ready"])
        self.assertEqual(row["outcome"], "CONTEXT_CANCELLED_BEFORE_READY")

    def test_otd_transition_to_oaor_cancels_before_ready(self) -> None:
        candidate = _candidate()
        timeline = build_dynamic_context_timeline(
            candidate,
            _context_history(inside_value=False),
        )
        first_cancel = next(
            point for point in timeline if point.cancellation_reason
        )
        row = replay_candidate(
            candidate,
            _context_history(inside_value=False),
        )

        self.assertEqual(
            first_cancel.cancellation_reason,
            "OTD_TRANSITIONED_TO_OAOR",
        )
        self.assertEqual(
            first_cancel.payload_updates["current_open_behavior"],
            "OPEN_AUCTION_OUT_OF_RANGE",
        )
        self.assertFalse(row["ready"])
        self.assertEqual(row["outcome"], "CONTEXT_CANCELLED_BEFORE_READY")

    def test_gross_net_cost_arithmetic_is_deterministic(self) -> None:
        ready_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
        outcome = _simulate_limit_outcome(
            _outcome_bars(
                [
                    (
                        "2026-07-01T10:05:00Z",
                        100.2,
                        100.5,
                        99.9,
                        100.3,
                    ),
                    (
                        "2026-07-01T10:10:00Z",
                        100.5,
                        102.1,
                        100.4,
                        102.0,
                    ),
                ]
            ),
            direction="LONG",
            ready_at=ready_at,
            entry_window_expires_at=ready_at + timedelta(minutes=30),
            trade_resolution_expires_at=ready_at + timedelta(hours=8),
            entry=100.0,
            stop=99.0,
            target=102.0,
            cost_model=ExecutionCostModel(
                spread_price=0.10,
                commission_r=0.05,
                adverse_exit_slippage_price=0.20,
                source="UNIT_TEST",
            ),
        )

        self.assertEqual(outcome["outcome"], "TP_HIT")
        self.assertAlmostEqual(outcome["gross_R"], 2.0)
        self.assertAlmostEqual(outcome["total_cost_R"], 0.15)
        self.assertAlmostEqual(outcome["net_R"], 1.85)
        self.assertAlmostEqual(outcome["adverse_slippage_cost_R"], 0.0)

    def test_cost_model_loader_requires_explicit_per_symbol_values(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="ltf-cost-model-") as tmp:
            path = Path(tmp) / "costs.json"
            path.write_text(
                json.dumps(
                    {
                        "symbols": {
                            "EURUSD": {
                                "spread_price": 0.00008,
                                "commission_r": 0.02,
                                "adverse_exit_slippage_price": 0.00003,
                                "limit_fill_policy": (
                                    "TRADE_THROUGH_HALF_SPREAD"
                                ),
                                "source": "BROKER_CALIBRATION_TEST",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            models = load_execution_cost_models(path)

        self.assertEqual(set(models), {"EURUSD"})
        self.assertAlmostEqual(models["EURUSD"].spread_price, 0.00008)
        self.assertEqual(
            models["EURUSD"].limit_fill_policy,
            "TRADE_THROUGH_HALF_SPREAD",
        )
        self.assertEqual(
            models["EURUSD"].source,
            "BROKER_CALIBRATION_TEST",
        )

    def test_report_exposes_chronological_holdout(self) -> None:
        candidate = _candidate()
        first = replay_candidate(candidate, _history())
        second = dict(first)
        second["candidate_id"] = "candidate-2"
        second["activated_at_utc"] = "2026-07-08T10:00:00+00:00"
        second["expires_at_utc"] = "2026-07-08T11:00:00+00:00"
        report = compile_backtest_report(
            candidates=[candidate],
            rows=[first, second],
            coverage=[
                {
                    "symbol": "EURUSD",
                    "history_first_bar_utc": "2026-07-01T00:00:00+00:00",
                    "history_last_bar_utc": "2026-07-08T23:55:00+00:00",
                }
            ],
            holdout_fraction=0.50,
        )

        self.assertEqual(report["split_index"], 1)
        self.assertEqual(report["metrics"]["development"]["candidate_count"], 1)
        self.assertEqual(report["metrics"]["holdout"]["candidate_count"], 1)
        self.assertFalse(report["research_scope"]["look_ahead_allowed"])
        self.assertTrue(
            report["execution_integrity"]["deadlines_are_separate"]
        )
        self.assertEqual(
            report["metrics"]["all"]["metric_basis"],
            "NET_R",
        )

    def test_monday_uses_last_completed_trading_session_not_sunday(self) -> None:
        friday_index = pd.date_range(
            "2026-07-03T07:00:00Z",
            periods=108,
            freq="5min",
        )
        friday = pd.DataFrame(
            {
                "open": [100.0] * len(friday_index),
                "high": [100.10] * len(friday_index),
                "low": [99.90] * len(friday_index),
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
                "high": [101.10] * len(monday_index),
                "low": [100.80] * len(monday_index),
                "close": [101.0] * len(monday_index),
                "volume": [1000.0] * len(monday_index),
            },
            index=monday_index,
        )
        monday.iloc[4] = [100.8, 100.9, 99.95, 100.2, 1000.0]
        monday.iloc[5] = [100.2, 100.9, 100.1, 100.8, 1000.0]
        history = pd.concat([friday, monday])

        candidates, audit = reconstruct_tpo_watch_candidates(
            history,
            symbol="EURUSD",
        )

        monday_candidates = [
            candidate
            for candidate in candidates
            if candidate.session_id.startswith("EURUSD_2026-07-06")
        ]
        self.assertEqual(len(monday_candidates), 1)
        self.assertIn("2026-07-03", monday_candidates[0].reference_profile_id)
        self.assertEqual(audit["diagnostics"].get("otd_candidates"), 1)

    def test_twelvedata_paginates_back_to_reported_earliest(self) -> None:
        pages = [
            {"datetime": "2026-07-01 00:00:00"},
            {
                "values": [
                    {
                        "datetime": "2026-07-01 00:10:00",
                        "open": "1.2",
                        "high": "1.3",
                        "low": "1.1",
                        "close": "1.25",
                    },
                    {
                        "datetime": "2026-07-01 00:15:00",
                        "open": "1.25",
                        "high": "1.35",
                        "low": "1.2",
                        "close": "1.3",
                    },
                ]
            },
            {
                "values": [
                    {
                        "datetime": "2026-07-01 00:00:00",
                        "open": "1.0",
                        "high": "1.1",
                        "low": "0.9",
                        "close": "1.05",
                    },
                    {
                        "datetime": "2026-07-01 00:05:00",
                        "open": "1.05",
                        "high": "1.2",
                        "low": "1.0",
                        "close": "1.2",
                    },
                ]
            },
        ]
        with patch(
            "scripts.run_ltf_execution_v2_backtest._request_json",
            side_effect=pages,
        ) as request:
            frame, audit = fetch_twelvedata_max_history(
                "EURUSD",
                api_key="not-written-to-artifact",
                end_utc=datetime(2026, 7, 1, 0, 15, tzinfo=UTC),
                pause_seconds=0.0,
            )

        self.assertEqual(len(frame), 4)
        self.assertEqual(request.call_count, 3)
        self.assertTrue(audit["reached_provider_earliest"])
        self.assertEqual(audit["provider_requests"], 3)
        self.assertNotIn("not-written-to-artifact", str(audit))

    def test_provider_fetch_requires_explicit_network_opt_in(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ltf-v2-provider-test-") as tmp:
            with self.assertRaises(ProviderDepthBacktestError):
                fetch_all_histories(
                    run_dir=Path(tmp),
                    symbols=["EURUSD"],
                    pause_seconds=0.0,
                    allow_network_fetch=False,
                    resume=False,
                )


if __name__ == "__main__":
    unittest.main()
