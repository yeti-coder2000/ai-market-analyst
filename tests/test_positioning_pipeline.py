from __future__ import annotations

import json
from pathlib import Path

from app.services.positioning.positioning_pipeline import refresh_positioning_runtime


def test_positioning_pipeline_persists_honest_no_data_snapshot(tmp_path: Path) -> None:
    result = refresh_positioning_runtime(
        runtime_dir=str(tmp_path),
        report_date="2026-07-20",
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
