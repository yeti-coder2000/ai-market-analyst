from __future__ import annotations

from dataclasses import replace
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
    ReconstructedProfile,
    _candidate_interest_zones,
    _simulate_limit_outcome,
    _two_block_acceptance,
    build_dynamic_context_timeline,
    compile_backtest_report,
    normalize_m5_history,
    reconstruct_tpo_watch_candidates,
    replay_candidate,
    run_history_backtest,
    summarize_backtest,
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
    write_report_artifacts,
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


def _gapped_context_history(*, inside_value: bool) -> pd.DataFrame:
    history = _context_history(inside_value=inside_value)
    return history.drop(pd.Timestamp("2026-07-01T10:55:00Z"))


def _gapped_three_block_context_history(
    *,
    inside_value: bool,
) -> pd.DataFrame:
    history = _context_history(inside_value=inside_value)
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
    third_index = pd.date_range(
        "2026-07-01T11:00:00Z",
        periods=6,
        freq="5min",
    )
    third = pd.DataFrame(
        {
            "open": [context_open] * len(third_index),
            "high": [context_high] * len(third_index),
            "low": [context_low] * len(third_index),
            "close": [context_close] * len(third_index),
            "volume": [1000.0] * len(third_index),
        },
        index=third_index,
    )
    return pd.concat([history, third]).drop(
        pd.Timestamp("2026-07-01T10:55:00Z")
    )


def _cash_reconstruction_history() -> pd.DataFrame:
    prior_index = pd.date_range(
        "2026-07-03T07:00:00Z",
        periods=108,
        freq="5min",
    )
    prior = pd.DataFrame(
        {
            "open": [100.0] * len(prior_index),
            "high": [100.10] * len(prior_index),
            "low": [99.90] * len(prior_index),
            "close": [100.0] * len(prior_index),
            "volume": [1000.0] * len(prior_index),
        },
        index=prior_index,
    )
    current_index = pd.date_range(
        "2026-07-06T07:00:00Z",
        periods=24,
        freq="5min",
    )
    current = pd.DataFrame(
        {
            "open": [115.0] * len(current_index),
            "high": [115.10] * len(current_index),
            "low": [114.80] * len(current_index),
            "close": [115.0] * len(current_index),
            "volume": [1000.0] * len(current_index),
        },
        index=current_index,
    )
    current.iloc[4] = [114.8, 114.9, 99.95, 114.2, 1000.0]
    current.iloc[5] = [114.2, 114.9, 114.1, 114.8, 1000.0]
    return pd.concat([prior, current])


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

    def test_entry_deadline_is_capped_by_trade_resolution_horizon(
        self,
    ) -> None:
        candidate = _candidate()
        expires_at = datetime(2026, 7, 1, 10, 40, tzinfo=UTC)
        candidate = replace(
            candidate,
            expires_at_utc=expires_at,
            payload={
                **candidate.payload,
                "expires_at_utc": expires_at.isoformat(),
            },
        )

        row = replay_candidate(candidate, _history())

        self.assertTrue(row["ready"])
        self.assertEqual(
            row["entry_window_expires_at_utc"],
            expires_at.isoformat(),
        )
        self.assertEqual(
            row["trade_resolution_expires_at_utc"],
            expires_at.isoformat(),
        )

    def test_fill_after_30_minute_entry_window_is_not_allowed(self) -> None:
        ready_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
        outcome = _simulate_limit_outcome(
            _outcome_bars(
                [
                    (
                        "2026-07-01T10:05:00Z",
                        100.2,
                        100.8,
                        100.1,
                        100.4,
                    ),
                    (
                        "2026-07-01T10:10:00Z",
                        100.4,
                        100.8,
                        100.1,
                        100.4,
                    ),
                    (
                        "2026-07-01T10:15:00Z",
                        100.4,
                        100.8,
                        100.1,
                        100.4,
                    ),
                    (
                        "2026-07-01T10:20:00Z",
                        100.4,
                        100.8,
                        100.1,
                        100.4,
                    ),
                    (
                        "2026-07-01T10:25:00Z",
                        100.4,
                        100.8,
                        100.1,
                        100.4,
                    ),
                    (
                        "2026-07-01T10:30:00Z",
                        100.4,
                        100.8,
                        100.1,
                        100.4,
                    ),
                    (
                        "2026-07-01T10:35:00Z",
                        100.4,
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

        self.assertEqual(outcome["outcome"], "ENTRY_WINDOW_EXPIRED_UNFILLED")
        self.assertIsNone(outcome["filled_at_utc"])
        self.assertEqual(
            outcome["resolved_at_utc"],
            "2026-07-01T10:30:00+00:00",
        )

    def test_later_gap_preserves_known_entry_window_expiry(self) -> None:
        ready_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
        outcome = _simulate_limit_outcome(
            _outcome_bars(
                [
                    (
                        f"2026-07-01T10:{minute:02d}:00Z",
                        100.4,
                        100.8,
                        100.1,
                        100.4,
                    )
                    for minute in range(5, 31, 5)
                ]
                + [
                    (
                        "2026-07-01T10:40:00Z",
                        100.4,
                        100.8,
                        100.1,
                        100.4,
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

        self.assertEqual(
            outcome["outcome"],
            "ENTRY_WINDOW_EXPIRED_UNFILLED",
        )
        self.assertEqual(
            outcome["resolved_at_utc"],
            "2026-07-01T10:30:00+00:00",
        )
        self.assertTrue(outcome["fill_evaluable"])
        self.assertEqual(
            outcome["execution_m5_integrity_status"],
            "COMPLETE_TO_CAUSAL_OUTCOME",
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
                        "2026-07-01T10:05:00Z",
                        100.4,
                        100.6,
                        100.2,
                        100.4,
                    ),
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

    def test_context_extreme_requires_close_beyond_tolerance(self) -> None:
        wick_only = _context_history(inside_value=False)
        wick_only.loc[
            pd.Timestamp("2026-07-01T10:00:00Z"),
            ["open", "high", "low", "close"],
        ] = [100.0, 100.2, 99.4, 100.0]
        close_break = wick_only.copy()
        close_break.loc[
            pd.Timestamp("2026-07-01T10:00:00Z"),
            ["open", "high", "low", "close"],
        ] = [100.0, 100.2, 99.0, 99.0]

        wick_timeline = build_dynamic_context_timeline(
            _candidate(),
            wick_only,
        )
        close_timeline = build_dynamic_context_timeline(
            _candidate(),
            close_break,
        )

        self.assertIsNone(wick_timeline[0].cancellation_reason)
        self.assertEqual(
            close_timeline[0].cancellation_reason,
            "INVALIDATED_BY_CONTEXT_EXTREME",
        )

    def test_filled_trade_can_resolve_after_entry_window(self) -> None:
        ready_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
        outcome = _simulate_limit_outcome(
            _outcome_bars(
                [
                    (
                        "2026-07-01T10:05:00Z",
                        100.3,
                        100.6,
                        100.2,
                        100.4,
                    ),
                    (
                        "2026-07-01T10:10:00Z",
                        100.4,
                        100.6,
                        100.2,
                        100.4,
                    ),
                    (
                        "2026-07-01T10:15:00Z",
                        100.4,
                        100.6,
                        100.2,
                        100.4,
                    ),
                    (
                        "2026-07-01T10:20:00Z",
                        100.4,
                        100.6,
                        100.2,
                        100.4,
                    ),
                    (
                        "2026-07-01T10:25:00Z",
                        100.2,
                        100.5,
                        99.9,
                        100.3,
                    ),
                    (
                        "2026-07-01T10:30:00Z",
                        100.3,
                        100.8,
                        100.2,
                        100.6,
                    ),
                    (
                        "2026-07-01T10:35:00Z",
                        100.6,
                        101.0,
                        100.5,
                        100.8,
                    ),
                    (
                        "2026-07-01T10:40:00Z",
                        100.8,
                        101.1,
                        100.7,
                        100.9,
                    ),
                    (
                        "2026-07-01T10:45:00Z",
                        100.9,
                        101.2,
                        100.8,
                        101.0,
                    ),
                    (
                        "2026-07-01T10:50:00Z",
                        101.0,
                        101.3,
                        100.9,
                        101.1,
                    ),
                    (
                        "2026-07-01T10:55:00Z",
                        101.1,
                        101.4,
                        101.0,
                        101.2,
                    ),
                    (
                        "2026-07-01T11:00:00Z",
                        101.2,
                        102.1,
                        101.1,
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

    def test_post_confirmation_gap_cannot_be_skipped_before_ready(
        self,
    ) -> None:
        history = _history().drop(
            pd.Timestamp("2026-07-01T10:05:00Z")
        )

        row = replay_candidate(_candidate(), history)

        self.assertFalse(row["ready"])
        self.assertEqual(
            row["outcome"],
            "NOT_EVALUABLE_INCOMPLETE_POST_CONFIRMATION_M5_SEQUENCE",
        )
        self.assertEqual(
            row["execution_m5_integrity_status"],
            "INCOMPLETE_POST_CONFIRMATION_M5_SEQUENCE",
        )
        self.assertFalse(row["ready_evaluable"])
        self.assertEqual(
            row["resolved_at_utc"],
            "2026-07-01T10:05:00+00:00",
        )

    def test_post_confirmation_duplicate_cannot_be_skipped_before_ready(
        self,
    ) -> None:
        history = _history()
        duplicated = pd.concat(
            [
                history,
                history.loc[
                    [pd.Timestamp("2026-07-01T10:05:00Z")]
                ],
            ]
        )

        row = replay_candidate(_candidate(), duplicated)

        self.assertFalse(row["ready"])
        self.assertEqual(
            row["outcome"],
            "NOT_EVALUABLE_DUPLICATE_POST_CONFIRMATION_M5_BAR",
        )
        self.assertEqual(
            row["execution_m5_integrity_status"],
            "DUPLICATE_POST_CONFIRMATION_M5_BAR",
        )
        self.assertFalse(row["ready_evaluable"])

    def test_gap_after_fill_cannot_credit_later_target(self) -> None:
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
                        "2026-07-01T10:15:00Z",
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
            trade_resolution_expires_at=ready_at + timedelta(hours=2),
            entry=100.0,
            stop=99.0,
            target=102.0,
        )

        self.assertEqual(
            outcome["outcome"],
            "NOT_EVALUABLE_INCOMPLETE_POST_CONFIRMATION_M5_SEQUENCE",
        )
        self.assertEqual(
            outcome["execution_m5_integrity_status"],
            "INCOMPLETE_POST_CONFIRMATION_M5_SEQUENCE",
        )
        self.assertEqual(
            outcome["filled_at_utc"],
            "2026-07-01T10:05:00+00:00",
        )
        self.assertIsNone(outcome["gross_R"])
        self.assertIsNone(outcome["net_R"])
        self.assertEqual(
            outcome["resolved_at_utc"],
            "2026-07-01T10:05:00+00:00",
        )

    def test_duplicate_after_fill_cannot_credit_later_target(
        self,
    ) -> None:
        ready_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
        duplicated = _outcome_bars(
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
                (
                    "2026-07-01T10:10:00Z",
                    100.5,
                    102.1,
                    100.4,
                    102.0,
                ),
            ]
        )

        outcome = _simulate_limit_outcome(
            duplicated,
            direction="LONG",
            ready_at=ready_at,
            entry_window_expires_at=ready_at + timedelta(minutes=30),
            trade_resolution_expires_at=ready_at + timedelta(hours=2),
            entry=100.0,
            stop=99.0,
            target=102.0,
        )

        self.assertEqual(
            outcome["outcome"],
            "NOT_EVALUABLE_DUPLICATE_POST_CONFIRMATION_M5_BAR",
        )
        self.assertEqual(
            outcome["execution_m5_integrity_status"],
            "DUPLICATE_POST_CONFIRMATION_M5_BAR",
        )
        self.assertEqual(
            outcome["filled_at_utc"],
            "2026-07-01T10:05:00+00:00",
        )
        self.assertIsNone(outcome["gross_R"])
        self.assertEqual(
            outcome["resolved_at_utc"],
            "2026-07-01T10:05:00+00:00",
        )

    def test_right_censoring_cannot_invent_entry_window_expiry(
        self,
    ) -> None:
        truncated = _history().loc[
            : pd.Timestamp("2026-07-01T10:15:00Z")
        ]

        row = replay_candidate(_candidate(), truncated)

        self.assertTrue(row["ready"])
        self.assertEqual(
            row["outcome"],
            "NOT_EVALUABLE_RIGHT_CENSORED_BEFORE_ENTRY_WINDOW",
        )
        self.assertEqual(
            row["execution_m5_integrity_status"],
            "RIGHT_CENSORED_BEFORE_ENTRY_WINDOW",
        )
        self.assertFalse(row["fill_evaluable"])
        self.assertFalse(row["trade_outcome_evaluable"])
        self.assertEqual(
            row["resolved_at_utc"],
            "2026-07-01T10:20:00+00:00",
        )
        summary = summarize_backtest([row])
        self.assertEqual(summary["ready_evaluable_candidate_count"], 1)
        self.assertEqual(summary["fill_evaluable_ready_count"], 0)
        self.assertIsNone(summary["fill_rate_of_ready"])

    def test_right_censoring_after_fill_cannot_invent_trade_expiry(
        self,
    ) -> None:
        truncated = _history().loc[
            : pd.Timestamp("2026-07-01T10:25:00Z")
        ]

        row = replay_candidate(_candidate(), truncated)

        self.assertTrue(row["ready"])
        self.assertEqual(
            row["outcome"],
            "NOT_EVALUABLE_RIGHT_CENSORED_BEFORE_TRADE_HORIZON",
        )
        self.assertEqual(
            row["execution_m5_integrity_status"],
            "RIGHT_CENSORED_BEFORE_TRADE_HORIZON",
        )
        self.assertTrue(row["fill_evaluable"])
        self.assertFalse(row["trade_outcome_evaluable"])
        self.assertEqual(
            row["filled_at_utc"],
            "2026-07-01T10:30:00+00:00",
        )
        self.assertEqual(
            row["resolved_at_utc"],
            "2026-07-01T10:30:00+00:00",
        )
        summary = summarize_backtest([row])
        self.assertEqual(summary["filled_count"], 1)
        self.assertEqual(
            summary["trade_outcome_evaluable_filled_count"],
            0,
        )
        self.assertEqual(summary["trade_outcome_unknown_filled_count"], 1)
        self.assertIsNone(summary["average_net_R_filled"])

    def test_right_censoring_before_ready_is_not_a_ready_failure(
        self,
    ) -> None:
        truncated = _history().loc[
            : pd.Timestamp("2026-07-01T10:10:00Z")
        ]

        row = replay_candidate(_candidate(), truncated)

        self.assertFalse(row["ready"])
        self.assertEqual(
            row["outcome"],
            "NOT_EVALUABLE_RIGHT_CENSORED_BEFORE_ENTRY_READY",
        )
        self.assertEqual(
            row["execution_m5_integrity_status"],
            "RIGHT_CENSORED_BEFORE_ENTRY_READY",
        )
        self.assertFalse(row["ready_evaluable"])
        self.assertEqual(
            row["resolved_at_utc"],
            "2026-07-01T10:15:00+00:00",
        )
        summary = summarize_backtest([row])
        self.assertEqual(summary["candidate_count"], 1)
        self.assertEqual(summary["ready_evaluable_candidate_count"], 0)
        self.assertIsNone(summary["ready_rate"])

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

    def test_incomplete_inside_value_block_does_not_confirm_acceptance(
        self,
    ) -> None:
        candidate = _candidate()
        timeline = build_dynamic_context_timeline(
            candidate,
            _gapped_context_history(inside_value=True),
        )

        self.assertFalse(
            any(point.cancellation_reason for point in timeline)
        )
        self.assertLessEqual(
            max(
                point.payload_updates["value_acceptance_tpo_count"]
                for point in timeline
            ),
            1,
        )
        self.assertTrue(
            all(
                point.payload_updates["current_open_behavior"]
                == "OPEN_TEST_DRIVE_CONFIRMED"
                for point in timeline
            )
        )

    def test_incomplete_outside_value_block_does_not_trigger_oaor(
        self,
    ) -> None:
        candidate = _candidate()
        timeline = build_dynamic_context_timeline(
            candidate,
            _gapped_context_history(inside_value=False),
        )

        self.assertFalse(
            any(point.cancellation_reason for point in timeline)
        )
        self.assertLessEqual(
            max(
                point.payload_updates["value_rejection_tpo_count"]
                for point in timeline
            ),
            1,
        )
        self.assertTrue(
            all(
                point.payload_updates["current_open_behavior"]
                == "OPEN_TEST_DRIVE_CONFIRMED"
                for point in timeline
            )
        )

    def test_gapped_block_breaks_later_acceptance_and_balance_streaks(
        self,
    ) -> None:
        candidate = _candidate()
        candidate = replace(
            candidate,
            expires_at_utc=datetime(
                2026,
                7,
                1,
                11,
                30,
                tzinfo=UTC,
            ),
            payload={
                **candidate.payload,
                "expires_at_utc": (
                    "2026-07-01T11:30:00+00:00"
                ),
            },
        )
        inside_timeline = build_dynamic_context_timeline(
            candidate,
            _gapped_three_block_context_history(inside_value=True),
        )
        outside_timeline = build_dynamic_context_timeline(
            candidate,
            _gapped_three_block_context_history(inside_value=False),
        )

        self.assertFalse(
            any(point.cancellation_reason for point in inside_timeline)
        )
        self.assertFalse(
            any(point.cancellation_reason for point in outside_timeline)
        )
        self.assertLessEqual(
            max(
                point.payload_updates["value_acceptance_tpo_count"]
                for point in inside_timeline
            ),
            1,
        )
        self.assertLessEqual(
            max(
                point.payload_updates["value_rejection_tpo_count"]
                for point in outside_timeline
            ),
            1,
        )

    def test_primary_orr_acceptance_requires_consecutive_complete_blocks(
        self,
    ) -> None:
        opens = pd.date_range(
            "2026-07-01T10:00:00Z",
            periods=18,
            freq="5min",
        )
        post_touch = pd.DataFrame(
            {
                "bar_open_utc": opens,
                "close": [99.0] * len(opens),
            }
        )
        gapped = post_touch.loc[
            post_touch["bar_open_utc"]
            != pd.Timestamp("2026-07-01T10:55:00Z")
        ]
        complete_two_blocks = post_touch.iloc[:12]

        accepted_gapped, activated_gapped = _two_block_acceptance(
            gapped,
            edge=100.0,
            opened_above=True,
            origin=datetime(2026, 7, 1, 10, 0, tzinfo=UTC),
        )
        accepted_complete, activated_complete = _two_block_acceptance(
            complete_two_blocks,
            edge=100.0,
            opened_above=True,
            origin=datetime(2026, 7, 1, 10, 0, tzinfo=UTC),
        )

        self.assertFalse(accepted_gapped)
        self.assertIsNone(activated_gapped)
        self.assertTrue(accepted_complete)
        self.assertEqual(
            activated_complete,
            datetime(2026, 7, 1, 11, 0, tzinfo=UTC),
        )

    def test_source_duplicate_rejects_context_and_primary_acceptance_block(
        self,
    ) -> None:
        outside = _context_history(inside_value=False)
        duplicated_context = pd.concat(
            [
                outside,
                outside.loc[
                    [pd.Timestamp("2026-07-01T10:05:00Z")]
                ],
            ]
        )
        timeline = build_dynamic_context_timeline(
            _candidate(),
            duplicated_context,
        )

        opens = pd.date_range(
            "2026-07-01T10:00:00Z",
            periods=12,
            freq="5min",
        )
        raw_acceptance = pd.DataFrame(
            {
                "bar_open_utc": opens,
                "open": [99.0] * len(opens),
                "high": [99.1] * len(opens),
                "low": [98.9] * len(opens),
                "close": [99.0] * len(opens),
            }
        )
        raw_acceptance = pd.concat(
            [raw_acceptance, raw_acceptance.iloc[[1]]],
            ignore_index=True,
        )
        normalized = normalize_m5_history(
            raw_acceptance,
            symbol="EURUSD",
        )
        accepted, activated = _two_block_acceptance(
            normalized,
            edge=100.0,
            opened_above=True,
            origin=datetime(2026, 7, 1, 10, 0, tzinfo=UTC),
        )

        self.assertFalse(any(point.cancellation_reason for point in timeline))
        self.assertLessEqual(
            max(
                point.payload_updates["value_rejection_tpo_count"]
                for point in timeline
            ),
            1,
        )
        self.assertFalse(accepted)
        self.assertIsNone(activated)

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

    def test_partial_cost_coverage_is_not_reported_as_complete(self) -> None:
        candidate = _candidate()
        explicit_row = replay_candidate(
            candidate,
            _history(),
            cost_model=ExecutionCostModel(source="EURUSD_TEST_COSTS"),
        )
        report = compile_backtest_report(
            candidates=[candidate],
            rows=[explicit_row],
            coverage=[
                {
                    "symbol": "EURUSD",
                    "history_first_bar_utc": (
                        "2026-07-01T00:00:00+00:00"
                    ),
                    "history_last_bar_utc": (
                        "2026-07-01T23:55:00+00:00"
                    ),
                },
                {
                    "symbol": "BTCUSD",
                    "history_first_bar_utc": (
                        "2026-07-01T00:00:00+00:00"
                    ),
                    "history_last_bar_utc": (
                        "2026-07-01T23:55:00+00:00"
                    ),
                },
            ],
        )
        integrity = report["execution_integrity"]

        self.assertEqual(
            integrity["cost_model_status"],
            "PARTIAL_PER_SYMBOL_CONFIG",
        )
        self.assertEqual(
            integrity["cost_model_explicit_symbols"],
            ["EURUSD"],
        )
        self.assertEqual(
            integrity["cost_model_missing_symbols"],
            ["BTCUSD"],
        )

    def test_zero_candidate_symbol_preserves_explicit_cost_coverage(
        self,
    ) -> None:
        report = run_history_backtest(
            {"BTCUSD": _history().iloc[:12]},
            cost_models={
                "BTCUSD": ExecutionCostModel(
                    spread_price=1.0,
                    commission_r=0.02,
                    source="BTC_EXPLICIT_TEST_COSTS",
                )
            },
        )

        self.assertEqual(report["metrics"]["all"]["candidate_count"], 0)
        self.assertEqual(
            report["execution_integrity"]["cost_model_status"],
            "EXPLICIT_PER_SYMBOL_CONFIG",
        )
        self.assertEqual(
            report["execution_integrity"][
                "cost_model_explicit_symbols"
            ],
            ["BTCUSD"],
        )
        self.assertEqual(
            report["execution_integrity"]["cost_model_missing_symbols"],
            [],
        )
        self.assertEqual(
            report["execution_cost_models"]["BTCUSD"]["source"],
            "BTC_EXPLICIT_TEST_COSTS",
        )
        self.assertEqual(
            report["metrics"]["by_symbol"]["BTCUSD"]["candidate_count"],
            0,
        )
        self.assertEqual(
            report["metrics"]["by_symbol"]["BTCUSD"][
                "ready_signals_per_week"
            ],
            0.0,
        )
        self.assertEqual(
            report["metrics"]["by_symbol"]["BTCUSD"][
                "frequency_coverage_scope"
            ],
            "ASSET_PROVIDER_COVERAGE:BTCUSD",
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

    def test_reconstruction_coverage_ends_at_exact_last_m5_close(self) -> None:
        _, audit = reconstruct_tpo_watch_candidates(
            _history(),
            symbol="EURUSD",
        )

        self.assertEqual(
            audit["history_last_bar_utc"],
            "2026-07-01T10:30:00+00:00",
        )
        self.assertEqual(
            audit["history_last_bar_close_utc"],
            "2026-07-01T10:35:00+00:00",
        )

    def test_zero_or_inverted_frequency_window_is_unavailable(self) -> None:
        row = replay_candidate(_candidate(), _history())
        boundary = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
        cases = {
            "zero": (
                boundary,
                boundary,
                "UNAVAILABLE_ZERO_LENGTH_WINDOW",
            ),
            "inverted": (
                boundary,
                boundary - timedelta(minutes=5),
                "UNAVAILABLE_INVERTED_WINDOW",
            ),
        }

        for name, (window_start, window_end, expected_status) in cases.items():
            with self.subTest(name=name):
                summary = summarize_backtest(
                    [row],
                    window_start=window_start,
                    window_end=window_end,
                )

                self.assertEqual(
                    summary["frequency_status"],
                    expected_status,
                )
                self.assertIsNone(summary["window_weeks"])
                self.assertIsNone(summary["ready_signals_per_week"])
                self.assertIsNone(summary["filled_signals_per_week"])

    def test_coverage_aware_frequency_never_infers_candidate_window(
        self,
    ) -> None:
        row = replay_candidate(_candidate(), _history())

        summary = summarize_backtest(
            [row],
            frequency_coverage_scope="ASSET_PROVIDER_COVERAGE:EURUSD",
        )

        self.assertEqual(
            summary["frequency_status"],
            "UNAVAILABLE_MISSING_WINDOW_BOUNDARY",
        )
        self.assertEqual(
            summary["frequency_coverage_scope"],
            "ASSET_PROVIDER_COVERAGE:EURUSD",
        )
        self.assertIsNone(summary["window_weeks"])
        self.assertIsNone(summary["ready_signals_per_week"])
        self.assertIsNone(summary["filled_signals_per_week"])

    def test_execution_universe_requires_unique_declared_coverage(
        self,
    ) -> None:
        row = replay_candidate(_candidate(), _history())
        valid_coverage = {
            "symbol": "EURUSD",
            "history_first_bar_utc": "2026-07-01T00:00:00+00:00",
            "history_last_bar_close_utc": "2026-07-02T00:00:00+00:00",
        }

        cases = {
            "missing": (
                [
                    {
                        **valid_coverage,
                        "symbol": "GBPUSD",
                    }
                ],
                "execution universe symbols missing declared coverage",
            ),
            "duplicate": (
                [valid_coverage, {**valid_coverage, "symbol": " eurusd "}],
                "duplicate declared coverage symbol=EURUSD",
            ),
            "blank": (
                [{**valid_coverage, "symbol": ""}],
                "coverage row is missing symbol",
            ),
        }

        for name, (coverage, error) in cases.items():
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, error):
                    compile_backtest_report(
                        candidates=[],
                        rows=[row],
                        coverage=coverage,
                    )

        noncanonical_row = {**row, "symbol": " eurusd "}
        with self.assertRaisesRegex(
            ValueError,
            "execution universe symbol is not canonical",
        ):
            compile_backtest_report(
                candidates=[],
                rows=[noncanonical_row],
                coverage=[valid_coverage],
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

    def test_prior_profile_gap_rejects_cash_candidate(self) -> None:
        history = _cash_reconstruction_history().drop(
            pd.Timestamp("2026-07-03T10:00:00Z")
        )

        candidates, audit = reconstruct_tpo_watch_candidates(
            history,
            symbol="GER40",
        )

        monday_candidates = [
            candidate
            for candidate in candidates
            if candidate.session_id.startswith("GER40_2026-07-06")
        ]
        self.assertEqual(monday_candidates, [])
        self.assertEqual(
            audit["diagnostics"].get(
                "skipped_prior_profile_incomplete_m5_sequence"
            ),
            1,
        )

    def test_corrupt_nearest_prior_session_blocks_older_profile_fallback(
        self,
    ) -> None:
        base_history = _cash_reconstruction_history()
        friday = base_history.loc[
            "2026-07-03T07:00:00Z":"2026-07-03T15:55:00Z"
        ].copy()
        thursday = friday.copy()
        thursday.index = thursday.index - pd.Timedelta(days=1)
        with_older_profile = pd.concat([thursday, base_history])
        corruptions = {
            "gap": with_older_profile.drop(
                pd.Timestamp("2026-07-03T10:00:00Z")
            ),
            "right_truncated": with_older_profile.drop(
                pd.date_range(
                    "2026-07-03T15:00:00Z",
                    periods=12,
                    freq="5min",
                )
            ),
            "duplicate": pd.concat(
                [
                    with_older_profile,
                    with_older_profile.loc[
                        [pd.Timestamp("2026-07-03T10:00:00Z")]
                    ],
                ]
            ),
        }

        for corruption, history in corruptions.items():
            with self.subTest(corruption=corruption):
                candidates, audit = reconstruct_tpo_watch_candidates(
                    history,
                    symbol="GER40",
                )

                monday_candidates = [
                    candidate
                    for candidate in candidates
                    if candidate.session_id.startswith(
                        "GER40_2026-07-06"
                    )
                ]
                self.assertEqual(monday_candidates, [])
                self.assertEqual(
                    audit["diagnostics"].get(
                        "skipped_prior_session_integrity_block"
                    ),
                    1,
                )

    def test_unconfirmed_empty_weekday_blocks_profile_fallback(
        self,
    ) -> None:
        history = _cash_reconstruction_history()
        monday = history.loc[
            "2026-07-06T07:00:00Z":"2026-07-06T08:55:00Z"
        ].copy()
        tuesday = monday.copy()
        tuesday.index = tuesday.index + pd.Timedelta(days=1)
        history = pd.concat(
            [
                history.loc[
                    "2026-07-03T07:00:00Z":"2026-07-03T15:55:00Z"
                ],
                tuesday,
            ]
        )

        candidates, audit = reconstruct_tpo_watch_candidates(
            history,
            symbol="GER40",
        )

        tuesday_candidates = [
            candidate
            for candidate in candidates
            if candidate.session_id.startswith("GER40_2026-07-07")
        ]
        self.assertEqual(tuesday_candidates, [])
        self.assertEqual(
            audit["diagnostics"].get(
                "skipped_prior_session_unconfirmed_no_data"
            ),
            1,
        )

    def test_crypto_weekend_is_not_skipped_as_non_trading_day(self) -> None:
        friday_index = pd.date_range(
            "2026-07-03T00:00:00Z",
            periods=288,
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
        sunday_index = pd.date_range(
            "2026-07-05T00:00:00Z",
            periods=24,
            freq="5min",
        )
        sunday = pd.DataFrame(
            {
                "open": [101.0] * len(sunday_index),
                "high": [101.1] * len(sunday_index),
                "low": [100.8] * len(sunday_index),
                "close": [101.0] * len(sunday_index),
                "volume": [1000.0] * len(sunday_index),
            },
            index=sunday_index,
        )

        candidates, audit = reconstruct_tpo_watch_candidates(
            pd.concat([friday, sunday]),
            symbol="BTCUSD",
        )

        sunday_candidates = [
            candidate
            for candidate in candidates
            if candidate.session_id.startswith("BTCUSD_2026-07-05")
        ]
        self.assertEqual(sunday_candidates, [])
        self.assertEqual(
            audit["diagnostics"].get(
                "skipped_prior_session_unconfirmed_no_data"
            ),
            1,
        )
        self.assertIsNone(
            audit["diagnostics"].get(
                "confirmed_non_trading_prior_days_skipped"
            )
        )

    def test_missing_prior_profile_open_rejects_cash_candidate(
        self,
    ) -> None:
        history = _cash_reconstruction_history().drop(
            pd.Timestamp("2026-07-03T07:00:00Z")
        )

        candidates, audit = reconstruct_tpo_watch_candidates(
            history,
            symbol="GER40",
        )

        monday_candidates = [
            candidate
            for candidate in candidates
            if candidate.session_id.startswith("GER40_2026-07-06")
        ]
        self.assertEqual(monday_candidates, [])
        self.assertEqual(
            audit["diagnostics"].get(
                "skipped_prior_profile_missing_exact_session_open_bar"
            ),
            1,
        )

    def test_complete_prior_profile_preserves_cash_candidate(self) -> None:
        candidates, audit = reconstruct_tpo_watch_candidates(
            _cash_reconstruction_history(),
            symbol="GER40",
        )

        monday_candidates = [
            candidate
            for candidate in candidates
            if candidate.session_id.startswith("GER40_2026-07-06")
        ]
        self.assertEqual(len(monday_candidates), 1)
        self.assertTrue(
            monday_candidates[0].payload[
                "event_census_execution_eligible"
            ]
        )
        self.assertEqual(
            monday_candidates[0].payload[
                "prior_profile_m5_integrity_status"
            ],
            "COMPLETE",
        )
        self.assertEqual(
            monday_candidates[0].payload["prior_profile_m5_bar_count"],
            108,
        )
        self.assertEqual(
            monday_candidates[0].payload[
                "prior_profile_expected_m5_bar_count"
            ],
            108,
        )
        self.assertEqual(
            monday_candidates[0].payload[
                "prior_profile_expected_right_edge_utc"
            ],
            "2026-07-03T16:00:00+00:00",
        )
        self.assertEqual(
            monday_candidates[0].payload[
                "prior_profile_last_bar_close_utc"
            ],
            "2026-07-03T16:00:00+00:00",
        )
        self.assertEqual(
            audit["session_spec"]["profile_duration_minutes"],
            540,
        )

    def test_right_truncated_prior_profile_rejects_cash_candidate(
        self,
    ) -> None:
        history = _cash_reconstruction_history()
        truncated_tail = pd.date_range(
            "2026-07-03T15:00:00Z",
            periods=12,
            freq="5min",
        )
        history = history.drop(truncated_tail)

        candidates, audit = reconstruct_tpo_watch_candidates(
            history,
            symbol="GER40",
        )

        monday_candidates = [
            candidate
            for candidate in candidates
            if candidate.session_id.startswith("GER40_2026-07-06")
        ]
        self.assertEqual(monday_candidates, [])
        self.assertEqual(
            audit["diagnostics"].get(
                "skipped_prior_profile_missing_confirmed_right_edge"
            ),
            1,
        )

    def test_prior_profile_duplicate_rejects_cash_candidate(self) -> None:
        history = _cash_reconstruction_history()
        duplicated = pd.concat(
            [
                history,
                history.loc[
                    [pd.Timestamp("2026-07-03T10:00:00Z")]
                ],
            ]
        )

        candidates, audit = reconstruct_tpo_watch_candidates(
            duplicated,
            symbol="GER40",
        )

        monday_candidates = [
            candidate
            for candidate in candidates
            if candidate.session_id.startswith("GER40_2026-07-06")
        ]
        self.assertEqual(monday_candidates, [])
        self.assertEqual(
            audit["diagnostics"].get(
                "skipped_prior_profile_duplicate_m5_bar"
            ),
            1,
        )

    def test_repeated_interest_zone_uses_newest_profile_metadata(
        self,
    ) -> None:
        older = ReconstructedProfile(
            session_id="EURUSD_2026-07-01_LONDON_SYNTHETIC",
            session_open_utc=datetime(2026, 7, 1, 7, 0, tzinfo=UTC),
            session_close_utc=datetime(2026, 7, 2, 7, 0, tzinfo=UTC),
            high=101.0,
            low=99.0,
            vah=100.5,
            val=99.5,
            poc=100.0,
            bin_width=0.1,
            bars=96,
        )
        newest = replace(
            older,
            session_id="EURUSD_2026-07-02_LONDON_SYNTHETIC",
            session_open_utc=datetime(2026, 7, 2, 7, 0, tzinfo=UTC),
            session_close_utc=datetime(2026, 7, 3, 7, 0, tzinfo=UTC),
        )

        zones = _candidate_interest_zones([older, newest])
        repeated_high = next(
            zone
            for zone in zones
            if zone["zone_type"] == "SESSION_HIGH"
            and zone["price"] == 101.0
        )

        self.assertEqual(
            repeated_high["profile_id"],
            newest.session_id,
        )

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

    def test_zero_event_rerun_removes_stale_census_artifacts(self) -> None:
        report = compile_backtest_report(
            candidates=[],
            rows=[],
            event_records=[],
            coverage=[],
        )
        with tempfile.TemporaryDirectory(
            prefix="ltf-v2-stale-census-"
        ) as tmp:
            run_dir = Path(tmp)
            stale_parquet = run_dir / "otd_orr_event_census.parquet"
            stale_csv = run_dir / "otd_orr_event_census.csv"
            stale_parquet_tmp = run_dir / (
                "otd_orr_event_census.parquet.tmp"
            )
            stale_csv_tmp = run_dir / "otd_orr_event_census.csv.tmp"
            stale_parquet.write_bytes(b"stale")
            stale_csv.write_text("stale\n", encoding="utf-8")
            stale_parquet_tmp.write_bytes(b"stale")
            stale_csv_tmp.write_text("stale\n", encoding="utf-8")

            artifacts = write_report_artifacts(
                run_dir=run_dir,
                report=report,
            )

            self.assertFalse(stale_parquet.exists())
            self.assertFalse(stale_csv.exists())
            self.assertFalse(stale_parquet_tmp.exists())
            self.assertFalse(stale_csv_tmp.exists())
            self.assertIsNone(artifacts["event_census_parquet"])
            self.assertIsNone(artifacts["event_census_csv"])


if __name__ == "__main__":
    unittest.main()
