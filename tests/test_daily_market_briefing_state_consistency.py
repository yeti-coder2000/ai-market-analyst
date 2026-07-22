from __future__ import annotations

import copy
import unittest

from app.services.daily_market_briefing import (
    _auction_subtype,
    _brief_symbol_context,
    _brief_verdict,
    _build_tpo_audit_snapshot,
    _post_news_symbol_line,
)


def _open_auction_item(*, accepted_outside: bool = False, legacy_otd: bool = False) -> dict:
    broad_behavior = "OPEN_TEST_DRIVE" if legacy_otd else "OPEN_AUCTION"
    value_state = "ACCEPTED_OUTSIDE_VALUE" if accepted_outside else "UNKNOWN"

    activity = {
        "ib_direction": "DOWN",
        "ib_extension_direction": "DOWN",
        "open_direction": "DOWN",
        "accepted_outside_value": accepted_outside,
    }
    behavior = {
        "open_context": "OPEN_INSIDE_VA",
        "open_behavior": broad_behavior,
        "initial_open_behavior": "OPEN_TEST_DRIVE_CANDIDATE",
        "current_open_behavior": "OPEN_AUCTION_IN_RANGE",
        "value_acceptance_state": value_state,
        "entry_model_hint": "WAIT_FOR_VALUE_REJECTION_AND_LTF_CONFIRMATION",
        "battle_bias_hint": "RESEARCH_ONLY",
        "profile_reliability_state": "RELIABLE",
        "true_otd_allowed": not legacy_otd,
        "first_hour_activity": activity,
    }
    context = copy.deepcopy(behavior)
    context.update({"market_status": "OPEN", "market_is_open": True, "open_relation": "OPEN_INSIDE_VA"})
    filters = copy.deepcopy(behavior)
    filters.update({"tpo_signal_permission": "OPEN_FOR_EVALUATION", "telegram_modifier": "DOWNGRADE"})
    return {"context": context, "filters": filters, "open_behavior": behavior}


class BriefingStateConsistencyTests(unittest.TestCase):
    def test_unresolved_open_auction_stays_observe_despite_directional_legacy_hints(self) -> None:
        item = _open_auction_item()
        data = _brief_symbol_context(item)

        self.assertEqual(data["open_behavior"], "OPEN_AUCTION")
        self.assertEqual(data["current_open_behavior"], "OPEN_AUCTION_IN_RANGE")
        self.assertEqual(data["tpo_watch_state"], "OBSERVE_ROTATION")
        self.assertEqual(_auction_subtype("XAUUSD", data, post_news_active=True), "BALANCE_CHOP")

        bucket, line = _brief_verdict("XAUUSD", data, post_news_active=True)
        self.assertEqual(bucket, "OBSERVE")
        self.assertIn("watch=OBSERVE_ROTATION/OPEN_AUCTION_IN_RANGE", line)
        self.assertNotIn("WAIT_FRESH_RETEST", line)

        reaction = _post_news_symbol_line("XAUUSD", item)
        self.assertIn("subtype=BALANCE_CHOP", reaction)
        self.assertIn("watch=OBSERVE_ROTATION/OPEN_AUCTION_IN_RANGE", reaction)
        self.assertIn("rotation/auction: без directional battle", reaction)
        self.assertNotIn("BEARISH_CONTINUATION_WAIT_RETEST", reaction)

    def test_current_open_auction_overrides_legacy_otd(self) -> None:
        item = _open_auction_item(legacy_otd=True)
        data = _brief_symbol_context(item)

        self.assertEqual(data["raw_open_behavior"], "OPEN_TEST_DRIVE")
        self.assertEqual(data["open_behavior"], "OPEN_AUCTION")
        self.assertEqual(data["tpo_watch_state"], "OBSERVE_ROTATION")
        self.assertEqual(_brief_verdict("BTCUSD", data, post_news_active=True)[0], "OBSERVE")

    def test_open_auction_directional_branch_can_reach_ltf_pending(self) -> None:
        item = _open_auction_item(accepted_outside=True)
        data = _brief_symbol_context(item)

        self.assertEqual(data["tpo_watch_state"], "LTF_MODEL_PENDING")
        self.assertEqual(data["tpo_watch_setup"], "OPEN_AUCTION_ACCEPTED_BREAKOUT")

        bucket, line = _brief_verdict("ETHUSD", data, post_news_active=True)
        self.assertEqual(bucket, "WATCH")
        self.assertIn("OPEN_AUCTION_BRANCH", line)
        self.assertIn("fresh retest + LTF model", line)

        reaction = _post_news_symbol_line("ETHUSD", item)
        self.assertIn("watch=LTF_MODEL_PENDING/OPEN_AUCTION_ACCEPTED_BREAKOUT", reaction)
        self.assertIn("directional branch підтверджена", reaction)

    def test_explicit_auction_blocker_remains_no_trade(self) -> None:
        item = _open_auction_item()
        item["filters"]["tpo_signal_permission"] = "BLOCKED_BY_AUCTION"
        data = _brief_symbol_context(item)

        bucket, line = _brief_verdict("XAUUSD", data, post_news_active=True)
        self.assertEqual(bucket, "NO_TRADE")
        self.assertIn("explicit blocker=BLOCKED_BY_AUCTION", line)

    def test_audit_snapshot_persists_resolved_watch_state(self) -> None:
        snapshot = _build_tpo_audit_snapshot(
            {
                "updated_at_utc": "2026-07-22T14:35:00+00:00",
                "symbols": {"XAUUSD": _open_auction_item(legacy_otd=True)},
            },
            "ny_1h",
        )
        row = snapshot["symbols"]["XAUUSD"]

        self.assertEqual(snapshot["version"], "tpo-audit-snapshot-v2-watch-state")
        self.assertEqual(row["open_behavior"], "OPEN_TEST_DRIVE")
        self.assertEqual(row["resolved_open_behavior"], "OPEN_AUCTION")
        self.assertEqual(row["resolved_current_open_behavior"], "OPEN_AUCTION_IN_RANGE")
        self.assertEqual(row["tpo_watch_state"], "OBSERVE_ROTATION")
        self.assertEqual(row["tpo_watch_setup"], "OPEN_AUCTION_IN_RANGE")
        self.assertIsNone(row["tpo_watch_bridge_error"])


if __name__ == "__main__":
    unittest.main()
