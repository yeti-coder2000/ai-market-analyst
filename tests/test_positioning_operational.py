from __future__ import annotations

import json
from pathlib import Path

from app.services.positioning.positioning_operational import (
    apply_operational_positioning_window,
)
from app.services.positioning.positioning_models import PositioningFeedItem
from app.services.positioning.positioning_tagger import interpret_positioning_item


def _snapshot(*, price: float, open_interest: float, generated_at: str) -> dict:
    return {
        "version": "test-live",
        "date": "2026-07-22",
        "generated_at": generated_at,
        "items": [
            {
                "symbol": "BTCUSD",
                "price": price,
                "open_interest": open_interest,
                "volume": 1000.0,
                "source": "kraken_futures_public_pf_xbtusd",
                "source_timestamp": generated_at,
                "flags": ["PERP_OI_PROXY"],
            }
        ],
    }


def test_operational_window_captures_morning_and_calculates_london_delta(tmp_path: Path) -> None:
    morning, morning_meta = apply_operational_positioning_window(
        _snapshot(price=100.0, open_interest=200.0, generated_at="2026-07-22T08:05:00+00:00"),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="morning_combined",
    )

    assert morning_meta["status"] == "BASELINE_CAPTURED"
    assert morning["items"][0]["price_change_pct"] == 0.0
    assert morning["items"][0]["open_interest_change_pct"] == 0.0
    morning_read = interpret_positioning_item(PositioningFeedItem.from_dict(morning["items"][0]))
    assert morning_read.primary_tag == "POSITIONING_NEUTRAL"
    assert "No London-session participation delta" in morning_read.interpretation
    baseline_path = tmp_path / "positioning" / "positioning_operational_morning_baseline.json"
    assert json.loads(baseline_path.read_text(encoding="utf-8"))["date"] == "2026-07-22"

    close, close_meta = apply_operational_positioning_window(
        _snapshot(price=105.0, open_interest=220.0, generated_at="2026-07-22T15:45:00+00:00"),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="london_close",
    )

    assert close_meta["status"] == "DELTA_READY"
    item = close["items"][0]
    assert item["price_change_pct"] == 5.0
    assert item["open_interest_change_pct"] == 10.0
    assert item["operational_window"]["status"] == "DELTA_READY"
    assert "OPERATIONAL_DELTA_SINCE_MORNING" in item["flags"]
    close_read = interpret_positioning_item(PositioningFeedItem.from_dict(item))
    assert close_read.primary_tag == "FRESH_LONG_PARTICIPATION"
    assert "Since the morning baseline" in close_read.interpretation


def test_operational_close_never_fabricates_delta_without_morning_baseline(tmp_path: Path) -> None:
    close, meta = apply_operational_positioning_window(
        _snapshot(price=105.0, open_interest=220.0, generated_at="2026-07-22T15:45:00+00:00"),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="london_close",
    )

    assert meta["status"] == "MORNING_BASELINE_MISSING"
    assert "operational_window" not in close["items"][0]


def test_operational_close_is_partial_when_a_morning_symbol_disappears(tmp_path: Path) -> None:
    morning_source = _snapshot(
        price=100.0,
        open_interest=200.0,
        generated_at="2026-07-22T08:05:00+00:00",
    )
    morning_source["items"].append(
        {
            "symbol": "ETHUSD",
            "price": 10.0,
            "open_interest": 400.0,
            "volume": 500.0,
            "source": "kraken_futures_public_pf_ethusd",
            "source_timestamp": "2026-07-22T08:05:00+00:00",
            "flags": ["PERP_OI_PROXY"],
        }
    )
    apply_operational_positioning_window(
        morning_source,
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="morning_combined",
    )

    _, meta = apply_operational_positioning_window(
        _snapshot(price=105.0, open_interest=220.0, generated_at="2026-07-22T15:45:00+00:00"),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="london_close",
    )

    assert meta["status"] == "PARTIAL"
    assert meta["ready_symbols"] == 1
    assert meta["missing_symbols"] == ["ETHUSD"]


def test_operational_baseline_rejects_stale_absolute_source(tmp_path: Path) -> None:
    source = _snapshot(
        price=100.0,
        open_interest=200.0,
        generated_at="2026-07-22T08:05:00+00:00",
    )
    source["items"][0]["flags"].append("STALE_SOURCE_DATA")

    _, meta = apply_operational_positioning_window(
        source,
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="morning_combined",
    )

    assert meta["status"] == "LIVE_SOURCE_UNAVAILABLE"
    assert not (tmp_path / "positioning" / "positioning_operational_morning_baseline.json").exists()
