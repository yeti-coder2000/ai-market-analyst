from __future__ import annotations

from unittest.mock import patch

from app.core.runtime_flags import tpo_runtime_enabled, tpo_telegram_enabled
from app.runners import main_worker
from app.services.daily_report_scheduler import load_config
from app.services.telegram_daily_report_sender import send_telegram_message
from app.services.telegram_notifier import TelegramNotifier


def test_legacy_tpo_flags_default_to_disabled(monkeypatch) -> None:
    monkeypatch.delenv("TPO_RUNTIME_ENABLED", raising=False)
    monkeypatch.delenv("TPO_TELEGRAM_ENABLED", raising=False)

    assert tpo_runtime_enabled() is False
    assert tpo_telegram_enabled() is False


def test_legacy_tpo_flags_are_reversible(monkeypatch) -> None:
    monkeypatch.setenv("TPO_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("TPO_TELEGRAM_ENABLED", "true")

    assert tpo_runtime_enabled() is True
    assert tpo_telegram_enabled() is True


def test_default_notifier_stays_inactive_when_tpo_telegram_is_disabled(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "test-chat")
    monkeypatch.setenv("TPO_TELEGRAM_ENABLED", "false")

    assert TelegramNotifier().is_active is False


def test_direct_daily_sender_does_not_open_network_when_disabled(monkeypatch) -> None:
    monkeypatch.setenv("TPO_TELEGRAM_ENABLED", "false")

    with patch("app.services.telegram_daily_report_sender.request.urlopen") as urlopen:
        result = send_telegram_message(
            bot_token="test-token",
            chat_id="test-chat",
            text="test",
        )

    assert result["ok"] is False
    assert "disabled" in result["error"]
    urlopen.assert_not_called()


def test_scheduler_requires_all_three_enable_flags(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_DAILY_REPORT_SCHEDULER", "true")
    monkeypatch.setenv("TPO_RUNTIME_ENABLED", "false")
    monkeypatch.setenv("TPO_TELEGRAM_ENABLED", "true")
    assert load_config().enabled is False

    monkeypatch.setenv("TPO_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("TPO_TELEGRAM_ENABLED", "false")
    assert load_config().enabled is False

    monkeypatch.setenv("TPO_TELEGRAM_ENABLED", "true")
    assert load_config().enabled is True


def test_main_worker_exits_before_runtime_when_disabled(monkeypatch) -> None:
    monkeypatch.setenv("TPO_RUNTIME_ENABLED", "false")

    with patch.object(main_worker, "run_analytics_cycle") as run_cycle:
        assert main_worker.main() == 0

    run_cycle.assert_not_called()
