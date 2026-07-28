from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from app.services.battle_permission import _ltf_model_executable
from app.services.ltf_execution_state_machine import (
    LTF_EXECUTION_STATE_MACHINE_VERSION,
    LTFExecutionStateStore,
    STATE_ARMED,
    STATE_ENTRY_READY,
    STATE_RETEST_HELD,
    STATE_RETEST_PENDING,
    STATE_TRIGGER_CONFIRMED,
    is_active_tpo_watch,
)
from app.services.signal_tracker import SignalTracker


def _bars() -> pd.DataFrame:
    rows = [
        # Closed pre-arm structure.
        ("2026-07-01T09:20:00Z", 100.00, 100.20, 99.90, 100.10),
        ("2026-07-01T09:25:00Z", 100.10, 100.25, 100.00, 100.15),
        ("2026-07-01T09:30:00Z", 100.15, 100.30, 100.05, 100.20),
        ("2026-07-01T09:35:00Z", 100.20, 100.35, 100.10, 100.25),
        ("2026-07-01T09:40:00Z", 100.25, 100.40, 100.15, 100.30),
        ("2026-07-01T09:45:00Z", 100.30, 100.42, 100.20, 100.28),
        ("2026-07-01T09:50:00Z", 100.28, 100.38, 100.18, 100.25),
        ("2026-07-01T09:55:00Z", 100.25, 100.40, 100.20, 100.30),
        # Post-arm impulse/BOS, wait, retest hold, then trigger.
        ("2026-07-01T10:00:00Z", 100.30, 101.30, 100.25, 101.20),
        ("2026-07-01T10:05:00Z", 101.20, 101.35, 100.90, 101.05),
        ("2026-07-01T10:10:00Z", 100.60, 100.90, 100.34, 100.80),
        ("2026-07-01T10:15:00Z", 100.78, 101.12, 100.72, 101.05),
        ("2026-07-01T10:20:00Z", 101.05, 101.15, 100.95, 101.08),
    ]
    index = pd.to_datetime([row[0] for row in rows], utc=True)
    return pd.DataFrame(
        {
            "open": [row[1] for row in rows],
            "high": [row[2] for row in rows],
            "low": [row[3] for row in rows],
            "close": [row[4] for row in rows],
            "volume": [1000.0] * len(rows),
        },
        index=index,
    )


def _payload(*, include_target: bool = True) -> dict[str, object]:
    payload: dict[str, object] = {
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
        "value_test_occurred": True,
        "previous_vah": 100.20,
        "previous_val": 98.50,
        "previous_poc": 99.30,
        "previous_low": 97.80,
        "current_session_id": "EURUSD_2026-07-01",
        "reference_profile_id": "EURUSD_2026-06-30",
        "context_invalidation_price": 99.50,
        "cycle_id": "2026-07-01T10:00:00+00:00",
        "expires_at_utc": "2026-07-01T12:00:00+00:00",
    }
    if include_target:
        payload["interest_zones"] = [
            {
                "zone_type": "NPOC",
                "role": "TARGET",
                "price": 103.00,
            }
        ]
    return payload


def _short_bars() -> pd.DataFrame:
    bars = _bars().copy()
    transformed = pd.DataFrame(index=bars.index)
    transformed["open"] = 200.0 - bars["open"]
    transformed["high"] = 200.0 - bars["low"]
    transformed["low"] = 200.0 - bars["high"]
    transformed["close"] = 200.0 - bars["close"]
    transformed["volume"] = bars["volume"]
    return transformed


def _short_payload() -> dict[str, object]:
    payload = _payload()
    payload.update(
        {
            "direction": "SHORT",
            "open_location": "OPEN_BELOW_VALUE",
            "previous_vah": 101.50,
            "previous_val": 99.80,
            "previous_poc": 100.70,
            "previous_high": 102.20,
            "context_invalidation_price": 100.50,
            "interest_zones": [
                {
                    "zone_type": "NPOC",
                    "role": "TARGET",
                    "price": 97.00,
                }
            ],
        }
    )
    return payload


class LtfExecutionStateMachineV2Test(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="ltf-v2-test-")
        self.state_path = Path(self.temporary.name) / "ltf_execution_state_v2.json"
        self.store = LTFExecutionStateStore(path=self.state_path)
        self.bars = _bars()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def evaluate(self, when: str, *, payload: dict[str, object] | None = None) -> dict[str, object]:
        return self.store.evaluate(
            payload or _payload(),
            df_5m=self.bars,
            as_of=when,
        )

    def test_causal_sequence_requires_post_bos_retest_and_later_trigger(self) -> None:
        armed = self.evaluate("2026-07-01T10:00:00Z")
        self.assertEqual(armed["ltf_execution_v2_state"], STATE_ARMED)
        self.assertFalse(armed["fresh_retest_exists"])
        self.assertEqual(armed["execution_status"], "NOT_EXECUTABLE")

        after_bos = self.evaluate("2026-07-01T10:05:00Z")
        self.assertEqual(
            after_bos["ltf_execution_v2_state"],
            STATE_RETEST_PENDING,
        )
        self.assertFalse(after_bos["fresh_retest_exists"])
        self.assertFalse(after_bos["ltf_model_confirmed"])
        self.assertEqual(after_bos["signal_class"], "WATCH")

        after_retest = self.evaluate("2026-07-01T10:15:00Z")
        self.assertEqual(
            after_retest["ltf_execution_v2_state"],
            STATE_RETEST_HELD,
        )
        self.assertTrue(after_retest["fresh_retest_exists"])
        self.assertFalse(after_retest["ltf_confirmed"])
        self.assertEqual(after_retest["execution_status"], "NOT_EXECUTABLE")

        ready = self.evaluate("2026-07-01T10:20:00Z")
        self.assertEqual(ready["ltf_execution_v2_state"], STATE_ENTRY_READY)
        self.assertEqual(ready["signal_class"], "READY")
        self.assertEqual(ready["execution_status"], "EXECUTABLE")
        self.assertTrue(ready["retest_confirmed"])
        self.assertTrue(ready["acceptance_confirmed"])
        self.assertTrue(ready["ltf_confirmed"])
        self.assertEqual(ready["entry_reference_price"], 100.42)
        self.assertLess(
            ready["invalidation_reference_price"],
            ready["entry_reference_price"],
        )
        self.assertGreater(
            ready["target_reference_price"],
            ready["entry_reference_price"],
        )
        self.assertGreaterEqual(ready["risk_reward_ratio"], 2.0)
        self.assertTrue(_ltf_model_executable(ready))
        tracker = SignalTracker(
            open_signals_path=Path(self.temporary.name) / "open_signals.json"
        )
        tracked = tracker.process(
            {**_payload(), **ready},
            cycle_id="cycle-ready",
        )
        self.assertEqual(tracked.payload["signal_class"], "READY")
        self.assertEqual(tracked.payload["execution_status"], "EXECUTABLE")
        self.assertEqual(tracked.payload["stop_quality"], "OK")

        transitions = [
            item["state"]
            for item in ready["ltf_execution_v2_transition_history"]
        ]
        self.assertEqual(
            transitions,
            [
                "ARMED",
                "IMPULSE_DETECTED",
                "BOS_CONFIRMED",
                "RETEST_PENDING",
                "RETEST_TOUCHED",
                "RETEST_HELD",
                "TRIGGER_CONFIRMED",
                "ENTRY_READY",
            ],
        )

    def test_bos_does_not_reuse_pre_bos_touch_as_fresh_retest(self) -> None:
        self.evaluate("2026-07-01T10:00:00Z")
        result = self.evaluate("2026-07-01T10:10:00Z")
        self.assertEqual(result["ltf_execution_v2_state"], STATE_RETEST_PENDING)
        self.assertFalse(result["fresh_retest_exists"])
        self.assertIsNone(result["ltf_execution_v2_retest_at_utc"])

    def test_trigger_without_real_target_stays_non_executable(self) -> None:
        payload = _payload(include_target=False)
        payload.pop("previous_high", None)
        payload.pop("previous_low", None)
        payload.pop("previous_poc", None)
        payload.pop("previous_val", None)
        payload.pop("previous_vah", None)

        self.evaluate("2026-07-01T10:00:00Z", payload=payload)
        self.evaluate("2026-07-01T10:05:00Z", payload=payload)
        self.evaluate("2026-07-01T10:15:00Z", payload=payload)
        result = self.evaluate("2026-07-01T10:20:00Z", payload=payload)

        self.assertEqual(
            result["ltf_execution_v2_state"],
            STATE_TRIGGER_CONFIRMED,
        )
        self.assertEqual(result["ltf_model_outcome"], "CONFIRMED_NEEDS_REAL_TARGET")
        self.assertEqual(result["signal_class"], "WATCH")
        self.assertEqual(result["execution_status"], "NOT_EXECUTABLE")

    def test_state_persists_and_same_setup_is_not_reborn(self) -> None:
        self.evaluate("2026-07-01T10:00:00Z")
        self.evaluate("2026-07-01T10:05:00Z")
        restored = LTFExecutionStateStore(path=self.state_path)
        result = restored.evaluate(
            _payload(),
            df_5m=self.bars,
            as_of="2026-07-01T10:15:00Z",
        )
        self.assertEqual(result["ltf_execution_v2_state"], STATE_RETEST_HELD)
        self.assertEqual(
            result["ltf_execution_state_machine_version"],
            LTF_EXECUTION_STATE_MACHINE_VERSION,
        )
        self.assertTrue(self.state_path.exists())

    def test_terminal_setup_is_one_shot_for_the_same_tpo_context(self) -> None:
        self.evaluate("2026-07-01T10:00:00Z")
        self.evaluate("2026-07-01T10:05:00Z")
        self.evaluate("2026-07-01T10:15:00Z")
        ready = self.evaluate("2026-07-01T10:20:00Z")
        setup_id = ready["ltf_execution_v2_setup_id"]

        expired = self.evaluate("2026-07-01T10:55:00Z")
        self.assertEqual(expired["ltf_execution_v2_state"], "EXPIRED")
        replayed = self.evaluate("2026-07-01T11:00:00Z")
        self.assertEqual(replayed["ltf_execution_v2_state"], "EXPIRED")
        self.assertEqual(replayed["ltf_execution_v2_setup_id"], setup_id)
        states = [
            item["state"]
            for item in replayed["ltf_execution_v2_transition_history"]
        ]
        self.assertEqual(states.count("ARMED"), 1)

    def test_short_path_builds_inverse_geometry(self) -> None:
        store = LTFExecutionStateStore(path=None)
        payload = _short_payload()
        bars = _short_bars()
        for when in (
            "2026-07-01T10:00:00Z",
            "2026-07-01T10:05:00Z",
            "2026-07-01T10:15:00Z",
            "2026-07-01T10:20:00Z",
        ):
            result = store.evaluate(
                payload,
                df_5m=bars,
                as_of=when,
                persist=False,
            )

        self.assertEqual(result["ltf_execution_v2_state"], STATE_ENTRY_READY)
        self.assertEqual(result["direction"], "SHORT")
        self.assertEqual(result["ltf_execution_v2_reference_level_label"], "VAL")
        self.assertGreater(
            result["invalidation_reference_price"],
            result["entry_reference_price"],
        )
        self.assertLess(
            result["target_reference_price"],
            result["entry_reference_price"],
        )
        self.assertGreaterEqual(result["risk_reward_ratio"], 2.0)

    def test_current_watch_branch_overrides_stale_legacy_otd_label(self) -> None:
        payload = _payload()
        payload.update(
            {
                "tpo_watch_setup": "OPEN_AUCTION_OUT_OF_RANGE_FAILED_ACCEPTANCE",
                "current_open_behavior": "OPEN_AUCTION_OUT_OF_RANGE",
                "open_behavior": "OPEN_TEST_DRIVE",
                "value_acceptance_state": "REJECTED_BACK_INTO_PRIOR_VALUE",
                "direction": "SHORT",
                "open_location": "OPEN_ABOVE_VALUE",
            }
        )
        result = LTFExecutionStateStore(path=None).evaluate(
            payload,
            df_5m=self.bars,
            as_of="2026-07-01T10:00:00Z",
            persist=False,
        )

        self.assertEqual(
            result["auction_ltf_setup"],
            "OPEN_AUCTION_BACK_TO_VALUE",
        )
        self.assertEqual(
            result["ltf_model_type"],
            "FAILED_ACCEPTANCE_BACK_TO_VALUE",
        )

    def test_inactive_watch_never_requests_execution(self) -> None:
        payload = _payload()
        payload["tpo_watch_state"] = "OBSERVE_ROTATION"
        payload["tpo_watch_active"] = False
        active, reason = is_active_tpo_watch(payload)
        self.assertFalse(active)
        self.assertIn("tpo_watch_state_not_active", reason)

        result = self.store.evaluate(
            payload,
            df_5m=self.bars,
            as_of=datetime(2026, 7, 1, 10, 20, tzinfo=UTC),
        )
        self.assertEqual(result["ltf_execution_v2_state"], "NO_ACTIVE_WATCH")
        self.assertEqual(result["execution_status"], "NOT_EXECUTABLE")

if __name__ == "__main__":
    unittest.main()
