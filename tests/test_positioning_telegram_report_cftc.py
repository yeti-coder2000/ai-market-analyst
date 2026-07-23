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


def test_positioning_telegram_report_renders_frankfurt_delta_previous_day_and_filters_inactive_us_assets() -> None:
    snapshot = {
        "date": "2026-07-22",
        "status": "OK",
        "operational_positioning": {
            "phase": "FRANKFURT_CONTROL",
            "status": "FRANKFURT_DELTA_READY",
            "baseline_timestamp": "2026-07-22T00:00:00+00:00",
            "symbols": {"BTCUSD": {}},
            "previous_trading_day": {
                "status": "AVAILABLE",
                "expected_date": "2026-07-21",
                "captured_at": "2026-07-21T20:15:00+00:00",
                "symbols": {
                    "BTCUSD": {
                        "price": 59500.0,
                        "open_interest": 148000.0,
                    }
                },
            },
        },
        "weekly_cot": {
            "status": "OK",
            "items": [
                {"symbol": "NAS100"},
                {"symbol": "XAUUSD", "positions": {}, "normalization": {}, "interpretation": {}, "data_quality": {}},
            ],
        },
        "items": [
            {
                "symbol": "BTCUSD",
                "daily_market_data": {
                    "price": 60000.0,
                    "open_interest": 149655.0,
                    "price_change_pct": 0.84,
                    "open_interest_change_pct": 1.12,
                },
                "market_proxy": {
                    "source": "kraken_futures_public_pi_xbtusd",
                    "operational_window": {
                        "status": "FRANKFURT_DELTA_READY",
                        "window": "japan_open_to_frankfurt",
                        "baseline_timestamp": "2026-07-22T00:00:00+00:00",
                        "current_timestamp": "2026-07-22T08:05:00+00:00",
                    },
                },
                "positioning_interpretation": {
                    "primary_tag": "POSITIONING_NEUTRAL",
                    "confidence": 0.35,
                    "data_quality": "MEDIUM",
                },
                "auction_usage": {
                    "battle_gate_impact": "none",
                    "telegram_signal_impact": "none",
                },
                "data_quality": {"status": "MEDIUM"},
            },
            {"symbol": "NAS100"},
        ],
    }

    text = render_positioning_telegram_message(snapshot=snapshot)

    assert "Window: Japan open → Frankfurt" in text
    assert "Previous trading-day close snapshot" in text
    assert "Date: <b>2026-07-21</b> | status: <b>AVAILABLE</b>" in text
    assert "Japan → Frankfurt" in text
    assert "Absolute snapshot: price 60,000.00 / OI 149,655.00" in text
    assert "XAUUSD · weekly COT" in text
    assert "NAS100" not in text
