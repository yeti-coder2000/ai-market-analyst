from __future__ import annotations

from app.services.tpo_context_exporter import _profile_scope, _profile_session_config
from app.services.daily_market_briefing import (
    _build_final_london_ny_profile_section,
    _build_tpo_audit_snapshot,
)
from app.services.telegram_daily_reporter import (
    _daily_close_tpo_path,
    _positioning_reference_date,
)


def test_daily_close_profile_uses_london_to_ny_scope(monkeypatch) -> None:
    monkeypatch.setenv("TPO_PROFILE_SCOPE", "LONDON_NY_COMBINED")

    config = _profile_session_config("XAUUSD")

    assert _profile_scope() == "LONDON_NY_COMBINED"
    assert config is not None
    assert config.session_anchor == "LONDON_NY_COMBINED_OPEN"
    assert config.timezone == "Europe/London"
    assert config.open_time == "08:00"
    assert config.primary_logic == "LONDON_NY_COMBINED_PROFILE"


def test_intraday_profile_keeps_instrument_native_scope(monkeypatch) -> None:
    monkeypatch.delenv("TPO_PROFILE_SCOPE", raising=False)

    assert _profile_scope() == "INSTRUMENT_NATIVE"
    assert _profile_session_config("XAUUSD") is None


def test_frankfurt_report_uses_previous_trading_date_for_positioning() -> None:
    assert _positioning_reference_date("morning_combined", "2026-07-23") == "2026-07-22"
    assert _positioning_reference_date("morning_combined", "2026-07-20") == "2026-07-17"
    assert _positioning_reference_date("daily_close", "2026-07-23") == "2026-07-23"


def test_daily_close_profile_has_separate_read_only_store(tmp_path) -> None:
    assert _daily_close_tpo_path(str(tmp_path)) == (
        tmp_path / "tpo" / "tpo_london_ny_close_latest.json"
    )


def test_daily_close_audit_derives_value_migration_and_renders_final_profile() -> None:
    snapshot = _build_tpo_audit_snapshot(
        {
            "symbols": {
                "XAUUSD": {
                    "symbol": "XAUUSD",
                    "previous_poc": 3300.0,
                    "current_poc": 3310.0,
                    "current_vah": 3320.0,
                    "current_val": 3290.0,
                    "current_high": 3330.0,
                    "current_low": 3280.0,
                    "current_open_behavior": "OPEN_DRIVE",
                    "behavior_transition": "OPEN_AUCTION_TO_OPEN_DRIVE",
                    "value_acceptance_state": "ACCEPTED_ABOVE_VALUE",
                }
            }
        },
        "daily_close",
    )

    row = snapshot["symbols"]["XAUUSD"]
    assert row["value_migration"] == "VALUE_MIGRATION_UP"
    text = "\n".join(_build_final_london_ny_profile_section(snapshot).lines)
    assert "POC 3310" in text
    assert "VAH/VAL 3320/3290" in text
    assert "VALUE_MIGRATION_UP" in text
