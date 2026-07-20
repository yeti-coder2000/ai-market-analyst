from __future__ import annotations

from app.services.positioning.positioning_telegram_report import (
    render_positioning_telegram_message,
)


def test_positioning_telegram_report_renders_weekly_cftc_section() -> None:
    snapshot = {
        "date": "2026-07-19",
        "status": "OK",
        "generated_at": "2026-07-19T18:00:00+00:00",
        "items": [],
        "weekly_cot": {
            "status": "OK",
            "report_date_latest": "2026-07-14",
            "items": [
                {
                    "symbol": "XAUUSD",
                    "report_date": "2026-07-14",
                    "report_age_days": 5,
                    "trader_group": "MANAGED_MONEY",
                    "positions": {
                        "project_net_contracts": 186682,
                        "weekly_change_net_contracts": -7564,
                        "net_pct_open_interest": 48.65,
                        "weekly_change_net_pct_open_interest": -2.17,
                    },
                    "normalization": {
                        "percentile": 94.2,
                        "zscore": 1.73,
                        "history_weeks": 156,
                    },
                    "interpretation": {
                        "primary_tag": "COT_EXTREME_NET_LONG",
                        "confidence": 0.75,
                        "text": "Speculative positioning is near its long extreme.",
                        "recommended_usage": "Use as slow weekly context only.",
                    },
                    "data_quality": {
                        "status": "GOOD",
                        "flags": ["CFTC_WEEKLY_COT", "NO_BATTLE_GATE_IMPACT"],
                    },
                }
            ],
        },
    }

    text = render_positioning_telegram_message(snapshot=snapshot)

    assert "Weekly CFTC COT" in text
    assert "Latest report: <b>2026-07-14</b>" in text
    assert "XAUUSD · weekly COT" in text
    assert "COT_EXTREME_NET_LONG" in text
    assert "Daily participation data unavailable" in text
    assert "Battle Gate: <b>none</b>" in text
