from __future__ import annotations

import unittest

from app.services.daily_market_briefing import _bias_without_trade


class DailyMarketBriefingTransitionWordingTest(unittest.TestCase):
    def test_wait_retest_bearish_state_is_candidate_not_active(self) -> None:
        wording = _bias_without_trade(
            "BTCUSD",
            {
                "market_status": "OPEN",
                "open_behavior": "OPEN_AUCTION",
                "modifier": "DOWNGRADE",
                "first_hour_activity": {"direction": "DOWN"},
            },
        )
        self.assertEqual(wording, "bearish continuation candidate / WAIT fresh retest")
        self.assertNotIn("active", wording)

    def test_wait_retest_bullish_state_is_candidate_not_active(self) -> None:
        wording = _bias_without_trade(
            "UKOIL",
            {
                "market_status": "OPEN",
                "open_behavior": "OPEN_AUCTION",
                "modifier": "DOWNGRADE",
                "first_hour_activity": {"direction": "UP"},
            },
        )
        self.assertEqual(wording, "bullish continuation candidate / WAIT fresh pullback")
        self.assertNotIn("active", wording)


if __name__ == "__main__":
    unittest.main()
