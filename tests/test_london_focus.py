from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from app.core.instrument_batches import get_batch_symbols
from app.runners.daily_reporting_worker import _should_send, _weekday_schedule
from app.services.daily_market_briefing import (
    LONDON_FOCUS_SYMBOLS,
    _build_london_close_comparison,
    _filter_london_focus_records,
    _macro_affected_symbols_text,
    _section_header_for_type,
    _symbol_scope_for_report,
)
from app.services.telegram_daily_reporter import (
    _positioning_close_skip_reason,
    _positioning_delivery_enabled,
)


def test_london_focus_deactivates_us_indices_and_oil_without_deleting_logic(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_NY_REPORT", "true")
    monkeypatch.delenv("ENABLE_LEGACY_NY_REPORT", raising=False)
    monkeypatch.delenv("ENABLE_LONDON_CLOSE_REPORT", raising=False)

    assert get_batch_symbols("indices") == ["GER40"]
    assert "USDCAD" in LONDON_FOCUS_SYMBOLS
    assert {"NAS100", "SPX500", "UKOIL"}.isdisjoint(LONDON_FOCUS_SYMBOLS)

    schedule = _weekday_schedule()
    assert [item.report_type for item in schedule] == ["morning_combined", "daily_close"]
    assert schedule[0].schedule_timezone == "Europe/Berlin"
    assert schedule[1].schedule_timezone == "America/New_York"
    assert schedule[1].refresh_tpo is True



def test_legacy_ny_schedule_requires_new_explicit_reactivation_flag(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_NY_REPORT", "true")
    monkeypatch.setenv("ENABLE_LEGACY_NY_REPORT", "true")

    assert [item.report_type for item in _weekday_schedule()] == [
        "morning_combined",
        "daily_close",
        "ny_1h",
    ]


def test_daily_close_schedule_is_dst_anchored_to_new_york(monkeypatch) -> None:
    monkeypatch.delenv("ENABLE_LEGACY_NY_REPORT", raising=False)
    close = next(item for item in _weekday_schedule() if item.report_type == "daily_close")
    state = {"sent": {}}

    assert not _should_send(datetime(2026, 7, 22, 20, 14, tzinfo=timezone.utc), close, state)
    assert _should_send(datetime(2026, 7, 22, 20, 15, tzinfo=timezone.utc), close, state)


def test_london_focus_report_scope_keeps_usd_macro_safety_without_us_assets() -> None:
    scope = _symbol_scope_for_report("london_close")
    assert scope == LONDON_FOCUS_SYMBOLS
    assert _section_header_for_type("london_close") == "🇬🇧 Підсумок London Close"

    text = _macro_affected_symbols_text(
        ["XAUUSD", "USDCAD", "NAS100", "SPX500", "BTCUSD"],
        "london_close",
    )
    assert "XAUUSD" in text
    assert "USDCAD" in text
    assert "BTCUSD" in text
    assert "NAS100" not in text
    assert "SPX500" not in text


def test_london_close_comparison_reads_persisted_morning_audit(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("RUNTIME_DIR", str(tmp_path))
    report_dir = tmp_path / "reports" / "briefings"
    report_dir.mkdir(parents=True)
    morning = {
        "version": "test-morning",
        "generated_at_utc": "2026-07-22T08:05:00+00:00",
        "raw": {
            "tpo_audit_snapshot": {
                "version": "tpo-audit-snapshot-v2-watch-state",
                "symbols": {
                    "XAUUSD": {
                        "resolved_current_open_behavior": "OPEN_AUCTION_IN_RANGE",
                        "tpo_watch_state": "OBSERVE_ROTATION",
                        "value_acceptance_state": "UNKNOWN",
                    }
                },
            }
        },
    }
    (report_dir / "2026-07-22_morning_combined.json").write_text(
        json.dumps(morning),
        encoding="utf-8",
    )
    close_snapshot = {
        "version": "tpo-audit-snapshot-v2-watch-state",
        "symbols": {
            "XAUUSD": {
                "resolved_current_open_behavior": "OPEN_DRIVE",
                "tpo_watch_state": "LTF_MODEL_PENDING",
                "value_acceptance_state": "ACCEPTED_OUTSIDE_VALUE",
            }
        },
    }

    section, audit = _build_london_close_comparison(
        tpo={"symbols": {}},
        target_date=datetime(2026, 7, 22).date(),
        close_snapshot=close_snapshot,
    )

    assert audit["status"] == "OK"
    assert audit["symbols"]["XAUUSD"]["changed"] is True
    assert "OPEN_AUCTION_IN_RANGE/OBSERVE_ROTATION → OPEN_DRIVE/LTF_MODEL_PENDING" in "\n".join(section.lines)


def test_london_close_positioning_skips_repeated_weekly_cot_without_operational_delta(tmp_path: Path) -> None:
    positioning_dir = tmp_path / "positioning"
    positioning_dir.mkdir(parents=True)
    latest_path = positioning_dir / "daily_positioning_latest.json"
    latest_path.write_text(
        json.dumps(
            {
                "operational_positioning": {
                    "status": "MORNING_BASELINE_MISSING",
                    "symbols": {},
                }
            }
        ),
        encoding="utf-8",
    )

    assert _positioning_close_skip_reason("london_close", runtime_dir=str(tmp_path)) == (
        "no_london_close_delta:morning_baseline_missing"
    )

    latest_path.write_text(
        json.dumps(
            {
                "operational_positioning": {
                    "status": "DELTA_READY",
                    "symbols": {"BTCUSD": {"price_change_pct": 1.0}},
                }
            }
        ),
        encoding="utf-8",
    )
    assert _positioning_close_skip_reason("london_close", runtime_dir=str(tmp_path)) is None


def test_london_close_positioning_is_not_disabled_by_legacy_type_list(monkeypatch) -> None:
    monkeypatch.setenv("REPORT_SEND_POSITIONING_TELEGRAM", "true")
    monkeypatch.setenv("REPORT_POSITIONING_TYPES", "morning,ny_1h")
    monkeypatch.delenv("REPORT_SEND_LONDON_CLOSE_POSITIONING", raising=False)

    assert _positioning_delivery_enabled("london_close") is True


def test_london_focus_statistics_start_new_cohort_without_deleting_legacy(monkeypatch) -> None:
    monkeypatch.setenv("LONDON_FOCUS_EFFECTIVE_DATE", "2026-07-23")
    records = [
        {"symbol": "XAUUSD", "created_at_utc": "2026-07-22T10:00:00+00:00"},
        {"symbol": "XAUUSD", "created_at_utc": "2026-07-23T10:00:00+00:00"},
        {"symbol": "NAS100", "created_at_utc": "2026-07-23T10:00:00+00:00"},
    ]

    cohort = _filter_london_focus_records(
        records,
        as_of_date=datetime(2026, 7, 23).date(),
        timezone_name="Europe/Kyiv",
    )

    assert cohort == [records[1]]
    assert records[2]["symbol"] == "NAS100"
