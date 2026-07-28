from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from app.services.ltf_execution_backtest import (
    HistoricalWatchCandidate,
    compile_backtest_report,
    reconstruct_tpo_watch_candidates,
    replay_candidate,
)
from scripts.run_ltf_execution_v2_backtest import (
    ProviderDepthBacktestError,
    fetch_all_histories,
    fetch_twelvedata_max_history,
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


class LtfExecutionBacktestTest(unittest.TestCase):
    def test_replay_is_causal_and_scores_future_limit_fill(self) -> None:
        candidate = _candidate()
        row = replay_candidate(candidate, _history())

        self.assertTrue(row["ready"])
        self.assertEqual(row["ready_at_utc"], "2026-07-01T10:20:00+00:00")
        self.assertEqual(row["filled_at_utc"], "2026-07-01T10:30:00+00:00")
        self.assertEqual(row["outcome"], "TP_HIT")
        self.assertGreaterEqual(row["realized_R"], 2.0)
        transition_times = [
            pd.Timestamp(item["at_utc"])
            for item in row["transition_history"]
        ]
        self.assertEqual(transition_times, sorted(transition_times))

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
