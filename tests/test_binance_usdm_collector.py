from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests

from app.services.positioning.collectors.binance_usdm_collector import (
    BinanceUsdmCollectorConfig,
    collect_and_write_binance_usdm_snapshot,
    collect_binance_usdm_snapshot,
)


class FakeResponse:
    def __init__(self, payload: Any, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self) -> Any:
        return self._payload


class FakeBinanceSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.now_ms = int(time.time() * 1000)

    def get(self, url: str, *, params: dict[str, Any], **kwargs: Any) -> FakeResponse:
        del kwargs
        self.calls.append((url, dict(params)))
        symbol = str(params.get("symbol") or "BTCUSDT")
        multiplier = 1.0 if symbol == "BTCUSDT" else 0.5

        if url.endswith("/fapi/v1/ticker/24hr"):
            return FakeResponse(
                {
                    "symbol": symbol,
                    "priceChangePercent": str(2.5 * multiplier),
                    "lastPrice": str(60000 * multiplier),
                    "quoteVolume": str(1200 * multiplier),
                    "closeTime": self.now_ms,
                }
            )

        if url.endswith("/fapi/v1/premiumIndex"):
            return FakeResponse(
                {
                    "symbol": symbol,
                    "markPrice": str(60010 * multiplier),
                    "indexPrice": str(60000 * multiplier),
                    "lastFundingRate": "0.0001",
                    "time": self.now_ms,
                }
            )

        if url.endswith("/fapi/v1/openInterest"):
            return FakeResponse(
                {
                    "symbol": symbol,
                    "openInterest": str(110 * multiplier),
                    "time": self.now_ms,
                }
            )

        if url.endswith("/futures/data/openInterestHist"):
            rows = []
            for hour in range(25):
                rows.append(
                    {
                        "symbol": symbol,
                        "sumOpenInterest": str((100 + hour * 0.2) * multiplier),
                        "timestamp": self.now_ms - (24 - hour) * 3_600_000,
                    }
                )
            return FakeResponse(rows)

        if url.endswith("/fapi/v1/klines"):
            rows = []
            for day_index in range(20):
                close_time = self.now_ms - (20 - day_index) * 86_400_000
                rows.append(
                    [
                        close_time - 86_399_999,
                        "1",
                        "1",
                        "1",
                        "1",
                        "1",
                        close_time,
                        str(1000 * multiplier),
                    ]
                )
            rows.append(
                [
                    self.now_ms,
                    "1",
                    "1",
                    "1",
                    "1",
                    "1",
                    self.now_ms + 86_399_999,
                    str(500 * multiplier),
                ]
            )
            return FakeResponse(rows)

        raise AssertionError(f"Unexpected URL: {url}")


class FailingSession:
    def get(self, *args: Any, **kwargs: Any) -> FakeResponse:
        del args, kwargs
        raise requests.ConnectionError("offline")


def test_collect_binance_usdm_snapshot_normalizes_public_data() -> None:
    session = FakeBinanceSession()
    config = BinanceUsdmCollectorConfig(
        base_url="https://example.test",
        timeout_sec=1.0,
        max_attempts=1,
        backoff_sec=0.0,
        volume_lookback_days=20,
        oi_history_hours=25,
    )

    snapshot = collect_binance_usdm_snapshot(
        target_date="2026-07-20",
        symbols=["BTCUSD"],
        config=config,
        session=session,
    )

    assert snapshot["collector"]["status"] == "OK"
    assert snapshot["collector"]["authentication"] == "none_public_market_data"
    assert snapshot["collector"]["battle_gate_impact"] == "none"
    assert snapshot["collector"]["telegram_signal_impact"] == "none"

    item = snapshot["items"][0]
    assert item["symbol"] == "BTCUSD"
    assert item["price_change_pct"] == 2.5
    assert item["open_interest_change_pct"] == 10.0
    assert item["volume_change_pct_vs_20d"] == 20.0
    assert item["funding_rate_pct"] == 0.01
    assert item["long_liquidations_usd"] is None
    assert item["short_liquidations_usd"] is None
    assert "LIQUIDATION_AGGREGATE_UNAVAILABLE" in item["flags"]
    assert item["binance_usdm"]["battle_gate_impact"] == "none"
    assert item["binance_usdm"]["telegram_signal_impact"] == "none"
    assert len(session.calls) == 5


def test_failed_live_refresh_does_not_overwrite_last_usable_snapshot(
    tmp_path: Path,
) -> None:
    positioning_dir = tmp_path / "positioning"
    positioning_dir.mkdir(parents=True)
    snapshot_path = positioning_dir / "crypto_derivatives_snapshot.json"
    previous = {
        "version": "previous-good",
        "date": "2026-07-19",
        "items": [{"symbol": "BTCUSD"}],
    }
    snapshot_path.write_text(json.dumps(previous), encoding="utf-8")

    config = BinanceUsdmCollectorConfig(
        base_url="https://example.test",
        timeout_sec=1.0,
        max_attempts=1,
        backoff_sec=0.0,
    )
    _, payload = collect_and_write_binance_usdm_snapshot(
        runtime_dir=str(tmp_path),
        target_date="2026-07-20",
        symbols=["BTCUSD"],
        config=config,
        session=FailingSession(),
        persist_empty=False,
    )

    assert payload["collector"]["status"] == "ERROR"
    assert payload["items"] == []
    assert json.loads(snapshot_path.read_text(encoding="utf-8")) == previous
