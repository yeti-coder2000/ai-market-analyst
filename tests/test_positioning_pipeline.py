from __future__ import annotations

import json
from pathlib import Path

from app.services.positioning.positioning_pipeline import refresh_positioning_runtime


def test_positioning_pipeline_persists_honest_no_data_snapshot(tmp_path: Path) -> None:
    result = refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-20",
        collect_live_crypto=False,
        collect_weekly_cot=False,
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
        collect_weekly_cot=False,
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
        collect_weekly_cot=False,
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


def test_positioning_pipeline_persists_weekly_cot_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from app.services.positioning import positioning_pipeline
    from app.services.positioning.positioning_store import write_json_atomic

    requested_contracts = []

    def fake_cot(**kwargs):
        requested_contracts.extend(kwargs.get("contracts") or [])
        path = Path(kwargs["output_path"])
        payload = {
            "version": "test-cftc",
            "date": "2026-07-19",
            "status": "OK",
            "report_date_latest": "2026-07-14",
            "items": [
                {
                    "symbol": "XAUUSD",
                    "report_date": "2026-07-14",
                    "history_weeks": 104,
                    "positions": {
                        "project_net_contracts": 100000,
                        "weekly_change_net_contracts": 5000,
                        "net_pct_open_interest": 24.0,
                        "weekly_change_net_pct_open_interest": 1.2,
                    },
                    "normalization": {"percentile": 92.0, "zscore": 1.8},
                    "interpretation": {
                        "primary_tag": "COT_EXTREME_NET_LONG",
                        "confidence": 0.75,
                        "battle_gate_impact": "none",
                        "telegram_signal_impact": "none",
                    },
                    "data_quality": {"status": "GOOD", "flags": ["CFTC_WEEKLY_COT"]},
                    "battle_gate_impact": "none",
                    "telegram_signal_impact": "none",
                }
            ],
            "collector": {
                "name": "cftc_official_cot_collector",
                "status": "OK",
                "symbols_collected": ["XAUUSD"],
                "errors": [],
                "warnings": [],
            },
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }
        write_json_atomic(path, payload)
        return path, payload

    monkeypatch.setattr(
        positioning_pipeline,
        "collect_and_write_cftc_cot_snapshot",
        fake_cot,
    )

    result = positioning_pipeline.refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-19",
        collect_live_crypto=False,
        collect_weekly_cot=True,
    )

    assert result["weekly_cot_status"] == "OK"
    assert result["weekly_cot_items"] == 1
    assert requested_contracts
    assert {spec.symbol for spec in requested_contracts}.isdisjoint({"NAS100", "SPX500", "UKOIL"})
    latest = json.loads(
        (tmp_path / "positioning" / "daily_positioning_latest.json").read_text(encoding="utf-8")
    )
    assert latest["weekly_cot"]["items"][0]["symbol"] == "XAUUSD"
    history = (tmp_path / "positioning" / "daily_positioning_history.jsonl").read_text(encoding="utf-8")
    assert '"weekly_cot"' in history


def test_positioning_pipeline_uses_stale_weekly_cot_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from app.services.positioning import positioning_pipeline
    from app.services.positioning.positioning_store import write_json_atomic

    cot_path = tmp_path / "positioning" / "cftc_weekly_positioning_snapshot.json"
    previous = {
        "version": "test-cftc",
        "date": "2026-07-12",
        "status": "OK",
        "report_date_latest": "2026-07-07",
        "items": [
            {
                "symbol": "EURUSD",
                "report_date": "2026-07-07",
                "positions": {"project_net_contracts": 1000},
                "normalization": {"history_weeks": 104},
                "interpretation": {
                    "primary_tag": "COT_NEUTRAL",
                    "battle_gate_impact": "none",
                    "telegram_signal_impact": "none",
                },
                "data_quality": {"status": "GOOD", "flags": ["CFTC_WEEKLY_COT"]},
                "battle_gate_impact": "none",
                "telegram_signal_impact": "none",
            }
        ],
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }
    write_json_atomic(cot_path, previous)

    def failed_cot(**kwargs):
        return Path(kwargs["output_path"]), {
            "version": "test-cftc",
            "date": "2026-07-19",
            "status": "ERROR",
            "items": [],
            "collector": {
                "status": "ERROR",
                "errors": ["provider unavailable"],
                "warnings": [],
            },
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }

    monkeypatch.setattr(
        positioning_pipeline,
        "collect_and_write_cftc_cot_snapshot",
        failed_cot,
    )

    result = positioning_pipeline.refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-19",
        collect_live_crypto=False,
        collect_weekly_cot=True,
    )

    assert result["weekly_cot_status"] == "STALE"
    latest = json.loads(
        (tmp_path / "positioning" / "daily_positioning_latest.json").read_text(encoding="utf-8")
    )
    assert latest["weekly_cot"]["status"] == "STALE"
    assert latest["weekly_cot"]["items"][0]["symbol"] == "EURUSD"
    assert latest["weekly_cot"]["runtime_fallback"]["reason"] == "live_cftc_refresh_unavailable"


def test_positioning_pipeline_falls_back_to_kraken_and_builds_london_delta(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from app.services.positioning import positioning_pipeline
    from app.services.positioning.positioning_store import write_json_atomic

    def failed_binance(**kwargs):
        return Path(kwargs["output_path"]), {
            "version": "test-binance-451",
            "date": "2026-07-22",
            "items": [],
            "collector": {
                "status": "ERROR",
                "errors": ["HTTP 451"],
                "warnings": [],
                "symbols_requested": ["BTCUSD", "ETHUSD"],
                "symbols_collected": [],
            },
        }

    values = {"price": 100.0, "oi": 200.0}

    def live_kraken(**kwargs):
        path = Path(kwargs["output_path"])
        payload = {
            "version": "test-kraken",
            "date": "2026-07-22",
            "generated_at": "2026-07-22T08:05:00+00:00",
            "items": [
                {
                    "symbol": "BTCUSD",
                    "price": values["price"],
                    "open_interest": values["oi"],
                    "volume": 1000.0,
                    "price_change_pct": 1.0,
                    "open_interest_change_pct": None,
                    "perp_open_interest_change_pct": None,
                    "source": "kraken_futures_public_pf_xbtusd",
                    "source_timestamp": "2026-07-22T08:05:00+00:00",
                    "flags": ["PERP_OI_PROXY", "CRYPTO_EXCHANGE_OI_NOISY"],
                },
                {
                    "symbol": "ETHUSD",
                    "price": values["price"] / 10.0,
                    "open_interest": values["oi"] * 2.0,
                    "volume": 500.0,
                    "price_change_pct": 1.0,
                    "open_interest_change_pct": None,
                    "perp_open_interest_change_pct": None,
                    "source": "kraken_futures_public_pf_ethusd",
                    "source_timestamp": "2026-07-22T08:05:00+00:00",
                    "flags": ["PERP_OI_PROXY", "CRYPTO_EXCHANGE_OI_NOISY"],
                },
            ],
            "collector": {
                "status": "OK",
                "symbols_collected": ["BTCUSD", "ETHUSD"],
                "errors": [],
                "warnings": [],
            },
        }
        write_json_atomic(path, payload)
        return path, payload

    monkeypatch.setattr(positioning_pipeline, "collect_and_write_binance_usdm_snapshot", failed_binance)
    monkeypatch.setattr(positioning_pipeline, "collect_and_write_kraken_futures_snapshot", live_kraken)

    write_json_atomic(
        tmp_path / "positioning" / "manual_daily_positioning_feed.json",
        {
            "date": "2026-07-22",
            "items": [
                {
                    "symbol": "BTCUSD",
                    "price_change_pct": 99.0,
                    "open_interest_change_pct": 99.0,
                    "source": "stale_manual_override",
                }
            ],
        },
    )

    japan = positioning_pipeline.refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="positioning_japan_open",
        collect_live_crypto=True,
        collect_weekly_cot=False,
    )
    assert japan["status"] == "OK"
    assert japan["operational_positioning_status"] == "JAPAN_BASELINE_CAPTURED"
    assert any(source["name"] == "kraken_futures_live" and source["status"] == "OK" for source in japan["sources"])

    values.update(price=102.0, oi=210.0)
    morning = positioning_pipeline.refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="morning_combined",
        collect_live_crypto=True,
        collect_weekly_cot=False,
    )
    assert morning["operational_positioning_status"] == "FRANKFURT_DELTA_READY"

    values.update(price=105.0, oi=220.0)
    close = positioning_pipeline.refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="london_1h",
        collect_live_crypto=True,
        collect_weekly_cot=False,
    )
    assert close["operational_positioning_status"] == "LONDON_1H_DELTA_READY"

    latest = json.loads(
        (tmp_path / "positioning" / "daily_positioning_latest.json").read_text(encoding="utf-8")
    )
    btc = next(item for item in latest["items"] if item["symbol"] == "BTCUSD")
    assert btc["daily_market_data"]["price_change_pct"] == 5.0
    assert btc["daily_market_data"]["open_interest_change_pct"] == 10.0
    assert btc["market_proxy"]["source"] == "kraken_futures_public_pf_xbtusd"
    assert latest["operational_positioning"]["status"] == "LONDON_1H_DELTA_READY"
    assert btc.get("positioning_can_allow_signal") is not True
    assert btc.get("positioning_can_block_signal") is not True
    assert btc["auction_usage"]["battle_gate_impact"] == "none"
    assert btc["auction_usage"]["telegram_signal_impact"] == "none"


def test_operational_report_does_not_reuse_previous_runtime_when_live_refresh_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from app.services.positioning import positioning_pipeline
    from app.services.positioning.positioning_store import write_json_atomic

    def failed_collector(**kwargs):
        return Path(kwargs["output_path"]), {
            "version": "test-live-error",
            "date": "2026-07-22",
            "items": [],
            "collector": {
                "status": "ERROR",
                "errors": ["provider unavailable"],
                "warnings": [],
                "symbols_requested": ["BTCUSD", "ETHUSD"],
                "symbols_collected": [],
            },
        }

    monkeypatch.setattr(
        positioning_pipeline,
        "collect_and_write_binance_usdm_snapshot",
        failed_collector,
    )
    monkeypatch.setattr(
        positioning_pipeline,
        "collect_and_write_kraken_futures_snapshot",
        failed_collector,
    )

    positioning_dir = tmp_path / "positioning"
    write_json_atomic(
        positioning_dir / "crypto_derivatives_snapshot.json",
        {
            "version": "old-runtime-snapshot",
            "date": "2026-07-22",
            "generated_at": "2026-07-22T08:05:00+00:00",
            "items": [
                {
                    "symbol": "BTCUSD",
                    "price": 60000.0,
                    "open_interest": 100.0,
                    "price_change_pct": 9.0,
                    "open_interest_change_pct": 9.0,
                    "source": "old_runtime_should_not_be_reused",
                }
            ],
            "collector": {"status": "OK"},
        },
    )
    write_json_atomic(
        positioning_dir / "manual_daily_positioning_feed.json",
        {
            "date": "2026-07-21",
            "items": [
                {
                    "symbol": "BTCUSD",
                    "price_change_pct": 8.0,
                    "open_interest_change_pct": 8.0,
                    "source": "stale_manual_should_not_be_reused",
                }
            ],
        },
    )

    result = positioning_pipeline.refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="morning_combined",
        collect_live_crypto=True,
        collect_weekly_cot=False,
    )

    assert result["operational_positioning_status"] == "LIVE_SOURCE_UNAVAILABLE"
    source_by_name = {source["name"]: source for source in result["sources"]}
    assert source_by_name["crypto_snapshot"]["status"] == "SKIPPED_NO_LIVE_REFRESH"
    assert source_by_name["manual_feed"]["status"] == "STALE"

    latest = json.loads(
        (positioning_dir / "daily_positioning_latest.json").read_text(encoding="utf-8")
    )
    assert latest["items"] == []
    assert latest["operational_positioning"]["status"] == "LIVE_SOURCE_UNAVAILABLE"
