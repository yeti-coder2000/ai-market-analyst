from __future__ import annotations

from typing import Any

from app.services.positioning.collectors.kraken_futures_collector import (
    KrakenFuturesCollectorConfig,
    collect_kraken_futures_snapshot,
)


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self.payload


class FakeKrakenSession:
    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        assert url.endswith("/tickers")
        assert kwargs["timeout"] == 1.0
        return FakeResponse(
            {
                "result": "success",
                "serverTime": "2026-07-22T08:00:00Z",
                "tickers": [
                    {
                        "tag": "perpetual",
                        "pair": "XBT:USD",
                        "symbol": "PF_XBTUSD",
                        "last": 60000,
                        "markPrice": 60010,
                        "indexPrice": 60000,
                        "openInterest": 100,
                        "volumeQuote": 1000000,
                        "fundingRate": 0.0001,
                        "change24h": 2.5,
                        "lastTime": "2026-07-22T07:59:59Z",
                    },
                    {
                        "tag": "perpetual",
                        "pair": "ETH:USD",
                        "symbol": "PF_ETHUSD",
                        "last": 3000,
                        "markPrice": 3001,
                        "indexPrice": 3000,
                        "openInterest": 200,
                        "volumeQuote": 500000,
                        "fundingRate": -0.0002,
                        "change24h": -1.0,
                        "lastTime": "2026-07-22T07:59:58Z",
                    },
                ],
            }
        )


def test_kraken_public_tickers_normalize_absolute_positioning_data() -> None:
    snapshot = collect_kraken_futures_snapshot(
        target_date="2026-07-22",
        config=KrakenFuturesCollectorConfig(
            base_url="https://example.test",
            timeout_sec=1.0,
        ),
        session=FakeKrakenSession(),
    )

    assert snapshot["collector"]["status"] == "OK"
    assert snapshot["collector"]["authentication"] == "none_public_market_data"
    assert snapshot["battle_gate_impact"] == "none"
    assert snapshot["telegram_signal_impact"] == "none"

    btc = next(item for item in snapshot["items"] if item["symbol"] == "BTCUSD")
    assert btc["price"] == 60000.0
    assert btc["open_interest"] == 100.0
    assert btc["open_interest_change_pct"] is None
    assert btc["funding_rate_pct"] == 0.01
    assert "KRAKEN_FUTURES_PUBLIC_DATA" in btc["flags"]
    assert btc["kraken_futures"]["battle_gate_impact"] == "none"


class PartialKrakenSession:
    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        del url, kwargs
        return FakeResponse(
            {
                "result": "success",
                "serverTime": "2026-07-22T08:00:00Z",
                "tickers": [
                    {
                        "tag": "perpetual",
                        "pair": "XBT:USD",
                        "symbol": "PF_XBTUSD",
                        "last": 60000,
                        "openInterest": 100,
                    },
                    {
                        "tag": "perpetual",
                        "pair": "ETH:USD",
                        "symbol": "PF_ETHUSD",
                        "last": 3000,
                        "openInterest": None,
                    },
                ],
            }
        )


def test_kraken_public_tickers_isolate_per_symbol_normalization_errors() -> None:
    snapshot = collect_kraken_futures_snapshot(
        target_date="2026-07-22",
        config=KrakenFuturesCollectorConfig(
            base_url="https://example.test",
            timeout_sec=1.0,
        ),
        session=PartialKrakenSession(),
    )

    assert snapshot["collector"]["status"] == "PARTIAL"
    assert [item["symbol"] for item in snapshot["items"]] == ["BTCUSD"]
    assert snapshot["collector"]["errors"][0].startswith("ETHUSD:ValueError:")


class StaleKrakenSession:
    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        del url, kwargs
        return FakeResponse(
            {
                "result": "success",
                "serverTime": "2026-07-22T08:00:00Z",
                "tickers": [
                    {
                        "tag": "perpetual",
                        "pair": "XBT:USD",
                        "symbol": "PF_XBTUSD",
                        "last": 60000,
                        "openInterest": 100,
                        "lastTime": "2026-07-22T06:00:00Z",
                    }
                ],
            }
        )


def test_kraken_public_tickers_flags_stale_source_time() -> None:
    snapshot = collect_kraken_futures_snapshot(
        target_date="2026-07-22",
        symbols=["BTCUSD"],
        config=KrakenFuturesCollectorConfig(
            base_url="https://example.test",
            timeout_sec=1.0,
        ),
        session=StaleKrakenSession(),
    )

    assert snapshot["collector"]["status"] == "PARTIAL"
    assert "STALE_SOURCE_DATA" in snapshot["items"][0]["flags"]
    assert snapshot["items"][0]["kraken_futures"]["source_age_minutes"] == 120.0
