from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from urllib import error

from app.runners.daily_reporting_worker import (
    ScheduledReport,
    _load_state,
    _mark_sent,
    _persist_delivery_progress,
)
from app.services.telegram_daily_reporter import _send_main_telegram_message
from app.services.telegram_notifier import TelegramConfig, TelegramNotifier


class _FakeNotifier:
    def __init__(self, results: list[bool]) -> None:
        self.results = list(results)
        self.calls: list[str] = []
        self.last_send_result: dict[str, object] | None = None

    def send_text(self, text: str) -> bool:
        self.calls.append(text)
        ok = self.results.pop(0)
        self.last_send_result = (
            {
                "ok": True,
                "http_status": 200,
                "retry_after": None,
            }
            if ok
            else {
                "ok": False,
                "http_status": 429,
                "error_code": 429,
                "description": "Too Many Requests: retry after 37",
                "retry_after": 37,
            }
        )
        return ok


def test_main_multipart_resume_sends_only_first_unsent_part() -> None:
    first = _FakeNotifier([True, False])
    progress: list[dict[str, object]] = []

    failed = _send_main_telegram_message(
        first,
        "",
        resume={
            "parts": ["main part 1", "main part 2"],
            "completed_parts": 0,
        },
        progress_callback=progress.append,
    )

    assert first.calls == ["main part 1", "main part 2"]
    assert failed["ok"] is False
    assert failed["completed_parts"] == 1
    assert failed["failed_part"] == 2
    assert failed["api_error"]["error_code"] == 429
    assert failed["retry_after"] == 37
    assert any(item["completed_parts"] == 1 for item in progress)

    retry = _FakeNotifier([True])
    completed = _send_main_telegram_message(
        retry,
        "",
        resume=failed,
    )

    assert retry.calls == ["main part 2"]
    assert completed["ok"] is True
    assert completed["completed_parts"] == 2
    assert completed["failed_part"] is None


def test_worker_persists_failed_part_api_error_and_clears_pending_on_success(
    monkeypatch,
    tmp_path,
) -> None:
    state_path = tmp_path / "daily_reporting_state.json"
    monkeypatch.setenv("DAILY_REPORTING_STATE_PATH", str(state_path))
    now = datetime(2026, 7, 24, 9, 1, tzinfo=timezone.utc)
    scheduled = ScheduledReport(
        "london_1h",
        "09:00",
        schedule_timezone="Europe/London",
    )
    state = _load_state()
    progress = {
        "stage": "main",
        "report_type": "london_1h",
        "report_date": "2026-07-24",
        "parts": ["part 1", "part 2"],
        "completed_parts": 1,
        "failed_part": 2,
        "api_error": {
            "error_code": 429,
            "description": "Too Many Requests",
            "retry_after": 37,
        },
        "retry_after": 37,
        "retry_after_utc": "2026-07-24T09:01:37+00:00",
    }

    _persist_delivery_progress(now, scheduled, progress, state)

    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    pending = persisted["pending_deliveries"]["2026-07-24:london_1h"]
    assert pending["completed_parts"] == 1
    assert pending["failed_part"] == 2
    assert pending["api_error"]["error_code"] == 429
    assert pending["retry_after"] == 37

    _mark_sent(
        now,
        scheduled,
        {"status": "ok", "telegram_sent": True},
        state,
    )
    completed = json.loads(state_path.read_text(encoding="utf-8"))
    assert "2026-07-24:london_1h" not in completed["pending_deliveries"]
    assert completed["sent"]["2026-07-24:london_1h"]["result"]["status"] == "ok"


def test_notifier_preserves_telegram_429_details(monkeypatch) -> None:
    payload = json.dumps(
        {
            "ok": False,
            "error_code": 429,
            "description": "Too Many Requests: retry after 23",
            "parameters": {"retry_after": 23},
        }
    ).encode("utf-8")

    def raise_rate_limit(*args, **kwargs):
        del args, kwargs
        raise error.HTTPError(
            url="https://api.telegram.org/test",
            code=429,
            msg="Too Many Requests",
            hdrs={"Retry-After": "23"},
            fp=io.BytesIO(payload),
        )

    monkeypatch.setattr(
        "app.services.telegram_notifier.request.urlopen",
        raise_rate_limit,
    )
    notifier = TelegramNotifier(
        TelegramConfig(
            enabled=True,
            bot_token="test-token",
            chat_id="test-chat",
            retries=3,
        )
    )

    assert notifier.send_text("test") is False
    assert notifier.last_send_result is not None
    assert notifier.last_send_result["error_code"] == 429
    assert notifier.last_send_result["description"] == "Too Many Requests: retry after 23"
    assert notifier.last_send_result["retry_after"] == 23
    assert notifier.last_send_result["attempt"] == 1
