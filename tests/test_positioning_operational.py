from __future__ import annotations

import json
from pathlib import Path

from app.services.positioning.positioning_models import PositioningFeedItem
from app.services.positioning.positioning_operational import (
    apply_operational_positioning_window,
)
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


def test_operational_cycle_builds_japan_frankfurt_and_london_windows(
    tmp_path: Path,
) -> None:
    previous_close, previous_meta = apply_operational_positioning_window(
        _snapshot(
            price=98.0,
            open_interest=190.0,
            generated_at="2026-07-21T20:15:00+00:00",
        ),
        runtime_dir=str(tmp_path),
        report_date="2026-07-21",
        report_type="daily_close",
    )
    assert previous_close["items"]
    assert previous_meta["status"] == "DAILY_CLOSE_SNAPSHOT_CAPTURED"

    japan, japan_meta = apply_operational_positioning_window(
        _snapshot(
            price=100.0,
            open_interest=200.0,
            generated_at="2026-07-22T00:00:00+00:00",
        ),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="positioning_japan_open",
    )
    assert japan_meta["status"] == "JAPAN_BASELINE_CAPTURED"
    assert japan["items"][0]["operational_window"]["window"] == "japan_open_baseline"

    frankfurt, frankfurt_meta = apply_operational_positioning_window(
        _snapshot(
            price=102.0,
            open_interest=210.0,
            generated_at="2026-07-22T06:05:00+00:00",
        ),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="morning_combined",
    )
    assert frankfurt_meta["status"] == "FRANKFURT_DELTA_READY"
    assert frankfurt_meta["previous_trading_day"]["status"] == "AVAILABLE"
    assert frankfurt["items"][0]["price_change_pct"] == 2.0
    assert frankfurt["items"][0]["open_interest_change_pct"] == 5.0
    assert (
        frankfurt["items"][0]["operational_window"]["window"]
        == "japan_open_to_frankfurt"
    )

    london, london_meta = apply_operational_positioning_window(
        _snapshot(
            price=105.0,
            open_interest=220.0,
            generated_at="2026-07-22T08:00:00+00:00",
        ),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="london_1h",
    )
    assert london_meta["status"] == "LONDON_1H_DELTA_READY"
    item = london["items"][0]
    assert item["price_change_pct"] == 5.0
    assert item["open_interest_change_pct"] == 10.0
    assert item["operational_window"]["frankfurt_change"]["price_change_pct"] == 2.941176
    assert item["operational_window"]["frankfurt_change"]["open_interest_change_pct"] == 4.761905
    assert "OPERATIONAL_DELTA_SINCE_JAPAN_OPEN" in item["flags"]

    read = interpret_positioning_item(PositioningFeedItem.from_dict(item))
    assert read.primary_tag == "FRESH_LONG_PARTICIPATION"
    assert "From Japan open to London +1h" in read.interpretation


def test_frankfurt_never_fabricates_japan_delta_but_still_captures_control_baseline(
    tmp_path: Path,
) -> None:
    frankfurt, meta = apply_operational_positioning_window(
        _snapshot(
            price=102.0,
            open_interest=210.0,
            generated_at="2026-07-22T06:05:00+00:00",
        ),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="morning_combined",
    )

    assert meta["status"] == "JAPAN_BASELINE_MISSING"
    assert frankfurt["items"][0]["price_change_pct"] is None
    assert (
        frankfurt["items"][0]["operational_window"]["status"]
        == "FRANKFURT_BASELINE_CAPTURED_NO_JAPAN"
    )
    baseline_path = (
        tmp_path
        / "positioning"
        / "positioning_operational_frankfurt_baseline.json"
    )
    assert json.loads(baseline_path.read_text(encoding="utf-8"))["date"] == "2026-07-22"


def test_london_control_is_partial_when_only_frankfurt_baseline_exists(
    tmp_path: Path,
) -> None:
    apply_operational_positioning_window(
        _snapshot(
            price=100.0,
            open_interest=200.0,
            generated_at="2026-07-22T06:05:00+00:00",
        ),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="morning_combined",
    )

    london, meta = apply_operational_positioning_window(
        _snapshot(
            price=105.0,
            open_interest=220.0,
            generated_at="2026-07-22T08:00:00+00:00",
        ),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="london_1h",
    )

    assert meta["status"] == "PARTIAL"
    assert "BTCUSD" in meta["symbols"]
    assert (
        london["items"][0]["operational_window"]["status"]
        == "FRANKFURT_ONLY_PARTIAL"
    )
    assert (
        london["items"][0]["operational_window"]["frankfurt_change"]["status"]
        == "FRANKFURT_TO_LONDON_1H_DELTA_READY"
    )
    assert london["items"][0].get("price_change_pct") is None


def test_previous_trading_day_snapshot_requires_exact_expected_date(
    tmp_path: Path,
) -> None:
    apply_operational_positioning_window(
        _snapshot(
            price=98.0,
            open_interest=190.0,
            generated_at="2026-07-20T20:15:00+00:00",
        ),
        runtime_dir=str(tmp_path),
        report_date="2026-07-20",
        report_type="daily_close",
    )
    _, meta = apply_operational_positioning_window(
        _snapshot(
            price=100.0,
            open_interest=200.0,
            generated_at="2026-07-22T06:05:00+00:00",
        ),
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="morning_combined",
    )

    assert meta["previous_trading_day"]["expected_date"] == "2026-07-21"
    assert meta["previous_trading_day"]["status"] == "MISSING"


def test_operational_baseline_rejects_stale_absolute_source(tmp_path: Path) -> None:
    source = _snapshot(
        price=100.0,
        open_interest=200.0,
        generated_at="2026-07-22T00:00:00+00:00",
    )
    source["items"][0]["flags"].append("STALE_SOURCE_DATA")

    _, meta = apply_operational_positioning_window(
        source,
        runtime_dir=str(tmp_path),
        report_date="2026-07-22",
        report_type="positioning_japan_open",
    )

    assert meta["status"] == "LIVE_SOURCE_UNAVAILABLE"
    assert not (
        tmp_path / "positioning" / "positioning_operational_japan_baseline.json"
    ).exists()
