from __future__ import annotations

import json
from pathlib import Path

from app.services.positioning.positioning_pipeline import refresh_positioning_runtime


def test_positioning_pipeline_persists_honest_no_data_snapshot(tmp_path: Path) -> None:
    result = refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-20",
        collect_live_crypto=False,
    )

    assert result["status"] == "NO_DATA"
    latest = tmp_path / "positioning" / "daily_positioning_latest.json"
    payload = json.loads(latest.read_text(encoding="utf-8"))
    assert payload["date"] == "2026-07-20"
    assert payload["status"] == "NO_DATA"
    assert payload["items"] == []


def test_positioning_pipeline_builds_manual_context(tmp_path: Path) -> None:
    positioning_dir = tmp_path / "positioning"
    positioning_dir.mkdir(parents=True)
    manual = {
        "version": "test",
        "date": "2026-07-19",
        "items": [
            {
                "symbol": "XAUUSD",
                "price_change_pct": 1.0,
                "open_interest_change_pct": 2.0,
                "volume_change_pct_vs_20d": 20.0,
                "source": "manual_test",
            }
        ],
    }
    (positioning_dir / "manual_daily_positioning_feed.json").write_text(
        json.dumps(manual),
        encoding="utf-8",
    )

    result = refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-20",
        collect_live_crypto=False,
    )

    assert result["status"] == "PARTIAL"
    assert result["items"] == 1
    latest = json.loads(
        (positioning_dir / "daily_positioning_latest.json").read_text(encoding="utf-8")
    )
    assert latest["date"] == "2026-07-20"
    assert latest["status"] == "PARTIAL"
    assert latest["items"][0]["symbol"] == "XAUUSD"
    assert latest["items"][0]["auction_usage"]["battle_gate_impact"] == "none"
    assert latest["items"][0]["auction_usage"]["telegram_signal_impact"] == "none"


def test_positioning_pipeline_builds_live_binance_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from app.services.positioning import positioning_pipeline
    from app.services.positioning.positioning_store import write_json_atomic

    def fake_collect(**kwargs):
        path = Path(kwargs["output_path"])
        payload = {
            "version": "test-live-binance",
            "date": "2026-07-20",
            "generated_at": "2026-07-20T08:00:00+00:00",
            "items": [
                {
                    "symbol": "BTCUSD",
                    "price_change_pct": 1.2,
                    "price": 60000.0,
                    "volume": 1000000.0,
                    "open_interest": 100.0,
                    "perp_open_interest_change_pct": 2.5,
                    "open_interest_change_pct": 2.5,
                    "volume_change_pct_vs_20d": 18.0,
                    "source": "binance_usdm_public_btcusdt",
                    "source_timestamp": "2026-07-20T08:00:00+00:00",
                    "flags": ["PERP_OI_PROXY", "CRYPTO_EXCHANGE_OI_NOISY"],
                },
                {
                    "symbol": "ETHUSD",
                    "price_change_pct": -0.8,
                    "price": 3000.0,
                    "volume": 500000.0,
                    "open_interest": 200.0,
                    "perp_open_interest_change_pct": 1.5,
                    "open_interest_change_pct": 1.5,
                    "volume_change_pct_vs_20d": 12.0,
                    "source": "binance_usdm_public_ethusdt",
                    "source_timestamp": "2026-07-20T08:00:00+00:00",
                    "flags": ["PERP_OI_PROXY", "CRYPTO_EXCHANGE_OI_NOISY"],
                },
            ],
            "collector": {
                "name": "binance_usdm_public_collector",
                "status": "OK",
                "symbols_requested": ["BTCUSD", "ETHUSD"],
                "symbols_collected": ["BTCUSD", "ETHUSD"],
                "errors": [],
                "battle_gate_impact": "none",
                "telegram_signal_impact": "none",
            },
        }
        write_json_atomic(path, payload)
        return path, payload

    monkeypatch.setattr(
        positioning_pipeline,
        "collect_and_write_binance_usdm_snapshot",
        fake_collect,
    )

    result = positioning_pipeline.refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-20",
        collect_live_crypto=True,
    )

    assert result["status"] == "OK"
    assert result["items"] == 2
    assert result["battle_gate_impact"] == "none"
    assert result["telegram_signal_impact"] == "none"

    latest = json.loads(
        (tmp_path / "positioning" / "daily_positioning_latest.json").read_text(
            encoding="utf-8"
        )
    )
    assert latest["status"] == "OK"
    assert {item["symbol"] for item in latest["items"]} == {"BTCUSD", "ETHUSD"}
    btc = next(item for item in latest["items"] if item["symbol"] == "BTCUSD")
    assert btc["daily_market_data"]["price"] == 60000.0
    assert btc["daily_market_data"]["open_interest"] == 100.0
    assert all(
        item["auction_usage"]["battle_gate_impact"] == "none"
        for item in latest["items"]
    )
