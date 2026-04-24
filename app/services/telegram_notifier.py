from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib import error, parse, request


logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3] + "..."


def _escape_html(text: Any) -> str:
    if text is None:
        return "-"
    s = str(text)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _normalize_target_zone(value: Any) -> str:
    if value is None:
        return "-"

    if isinstance(value, (list, tuple)):
        cleaned = [str(x).strip() for x in value if str(x).strip()]
        return ", ".join(_escape_html(x) for x in cleaned) if cleaned else "-"

    if isinstance(value, dict):
        try:
            return _escape_html(json.dumps(value, ensure_ascii=False, sort_keys=True))
        except Exception:
            return _escape_html(str(value))

    text = str(value).strip()
    return _escape_html(text) if text else "-"


def _infer_alert_type(payload: Dict[str, Any]) -> str:
    """
    Infer Telegram alert type from payload state.

    This function is intentionally module-level because both the class method
    and standalone helper may use it.
    """
    explicit = str(payload.get("alert_type", "")).strip().upper()
    if explicit:
        return explicit

    signal_class = str(
        payload.get("signal_class")
        or payload.get("stage")
        or payload.get("current_stage")
        or ""
    ).strip().upper()

    execution_status = str(payload.get("execution_status") or "").strip().upper()

    if signal_class == "ACTIVE":
        return "TRIGGERED"

    if signal_class == "READY":
        return "ENTRY_READY"

    if execution_status == "EXECUTABLE":
        return "ENTRY_READY"

    if signal_class == "WATCH":
        return "WATCH_NEW"

    if signal_class == "RESOLVED":
        resolution = str(payload.get("resolution") or payload.get("resolution_reason") or "").upper()
        if resolution == "INVALIDATED":
            return "INVALIDATED"

    return ""


def _normalize_alert_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize alert payload v2 into Telegram-compatible fields.

    Keeps original keys and adds compatibility aliases expected by the formatter.
    """
    normalized = dict(payload)

    alert_type = _infer_alert_type(normalized)
    if alert_type:
        normalized["alert_type"] = alert_type

    normalized.setdefault(
        "scenario_type",
        normalized.get("scenario") or normalized.get("scenario_type") or "UNKNOWN",
    )
    normalized.setdefault(
        "watch_reason",
        normalized.get("rationale") or normalized.get("reason") or "-",
    )
    normalized.setdefault(
        "scenario_probability",
        normalized.get("confidence") or normalized.get("scenario_probability") or 0.0,
    )
    normalized.setdefault(
        "invalidation_level",
        normalized.get("invalidation_reference_price"),
    )

    if "target_zone" not in normalized:
        target = normalized.get("target_reference_price")
        normalized["target_zone"] = [target] if target is not None else []

    return normalized


@dataclass
class TelegramConfig:
    enabled: bool
    bot_token: str
    chat_id: str
    parse_mode: str = "HTML"
    disable_web_page_preview: bool = True
    timeout_seconds: int = 10
    retries: int = 3
    retry_delay_seconds: float = 2.0
    paper_mode_prefix: str = "🧪 PAPER"
    live_mode_prefix: str = "🚨 LIVE"
    max_message_length: int = 3900
    allowed_alert_types: tuple[str, ...] = (
        "WATCH_NEW",
        "WATCH_UPGRADED",
        "TRIGGERED",
        "ENTRY_READY",
        "INVALIDATED",
    )


class TelegramNotifier:
    """
    Production-ready Telegram notifier for AI Market Analyst.

    Supported interfaces:
    - send_text(text)
    - send_admin_message(text)
    - send_alert(payload)
    - send_alert_payload(payload)

    Compatibility properties:
    - is_enabled
    - is_active
    """

    def __init__(self, config: Optional[TelegramConfig] = None) -> None:
        self.config = config or TelegramConfig(
            enabled=_env_bool("TELEGRAM_ENABLED", False),
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            parse_mode=(os.getenv("TELEGRAM_PARSE_MODE", "HTML").strip() or "HTML"),
            timeout_seconds=_safe_int(os.getenv("TELEGRAM_TIMEOUT_SECONDS", "10"), 10),
            retries=_safe_int(os.getenv("TELEGRAM_RETRIES", "3"), 3),
            retry_delay_seconds=float(os.getenv("TELEGRAM_RETRY_DELAY_SECONDS", "2")),
            paper_mode_prefix=os.getenv("TELEGRAM_PAPER_PREFIX", "🧪 PAPER").strip() or "🧪 PAPER",
            live_mode_prefix=os.getenv("TELEGRAM_LIVE_PREFIX", "🚨 LIVE").strip() or "🚨 LIVE",
            max_message_length=_safe_int(os.getenv("TELEGRAM_MAX_MESSAGE_LENGTH", "3900"), 3900),
        )

    @property
    def is_enabled(self) -> bool:
        return (
            self.config.enabled
            and bool(self.config.bot_token)
            and bool(self.config.chat_id)
        )

    @property
    def is_active(self) -> bool:
        return self.is_enabled

    def send_text(self, text: str) -> bool:
        if not self.is_enabled:
            logger.info("Telegram notifier disabled or not configured.")
            return False

        safe_text = _truncate(text, self.config.max_message_length)
        url = f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"

        payload = {
            "chat_id": self.config.chat_id,
            "text": safe_text,
            "parse_mode": self.config.parse_mode,
            "disable_web_page_preview": self.config.disable_web_page_preview,
        }

        encoded = parse.urlencode(payload).encode("utf-8")
        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.retries + 1):
            try:
                req = request.Request(url, data=encoded, method="POST")
                with request.urlopen(req, timeout=self.config.timeout_seconds) as resp:
                    body = resp.read().decode("utf-8", errors="replace")

                    if 200 <= resp.status < 300:
                        logger.info(
                            "Telegram message sent successfully. attempt=%s status=%s",
                            attempt,
                            resp.status,
                        )
                        logger.debug("Telegram response body=%s", body)
                        return True

                    logger.error(
                        "Telegram send failed. attempt=%s status=%s body=%s",
                        attempt,
                        resp.status,
                        body,
                    )

            except error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
                logger.error(
                    "Telegram HTTPError. attempt=%s/%s code=%s body=%s",
                    attempt,
                    self.config.retries,
                    exc.code,
                    body,
                )
                last_error = exc

            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Telegram send exception. attempt=%s/%s",
                    attempt,
                    self.config.retries,
                )
                last_error = exc

            if attempt < self.config.retries:
                time.sleep(self.config.retry_delay_seconds)

        logger.error("Telegram message failed after retries. last_error=%s", last_error)
        return False

    def send_admin_message(self, text: str) -> bool:
        return self.send_text(text)

    def send_alert(self, payload: Dict[str, Any]) -> bool:
        return self.send_alert_payload(payload)

    def send_alert_payload(self, payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            logger.warning("send_alert_payload received non-dict payload.")
            return False

        should_alert = bool(payload.get("should_alert", False))
        if not should_alert:
            logger.debug("alert_payload.should_alert=False, Telegram send skipped.")
            return False

        normalized_payload = _normalize_alert_payload(payload)
        alert_type = str(normalized_payload.get("alert_type", "")).strip().upper()

        if not alert_type:
            logger.info(
                "Alert type missing and could not be inferred. symbol=%s signal_class=%s execution_status=%s",
                normalized_payload.get("symbol"),
                normalized_payload.get("signal_class"),
                normalized_payload.get("execution_status"),
            )
            return False

        if self.config.allowed_alert_types and alert_type not in self.config.allowed_alert_types:
            logger.info(
                "Alert type not allowed for Telegram delivery. alert_type=%s symbol=%s signal_id=%s",
                alert_type,
                normalized_payload.get("symbol"),
                normalized_payload.get("signal_id"),
            )
            return False

        message = self.format_alert_payload(normalized_payload)

        logger.info(
            "Sending Telegram alert. symbol=%s alert_type=%s signal_id=%s",
            normalized_payload.get("symbol"),
            alert_type,
            normalized_payload.get("signal_id"),
        )
        return self.send_text(message)

    def format_alert_payload(self, payload: Dict[str, Any]) -> str:
        symbol = _escape_html(payload.get("symbol", "UNKNOWN"))
        alert_type = _escape_html(payload.get("alert_type", "UNKNOWN"))
        scenario_type = _escape_html(payload.get("scenario_type", "UNKNOWN"))
        direction = _escape_html(payload.get("direction", "UNKNOWN"))
        watch_reason = _escape_html(payload.get("watch_reason", "-"))
        market_state = _escape_html(payload.get("market_state", "-"))
        htf_bias = _escape_html(payload.get("htf_bias", "-"))
        invalidation_level = payload.get("invalidation_level")
        target_zone = payload.get("target_zone")
        probability = _safe_float(payload.get("scenario_probability"), 0.0)
        paper_mode = bool(payload.get("paper_mode", True))
        cycle_id = _escape_html(payload.get("cycle_id", "-"))

        execution_status = _escape_html(payload.get("execution_status", "-"))
        execution_model = _escape_html(payload.get("execution_model", "-"))
        entry_reference_price = payload.get("entry_reference_price")
        invalidation_reference_price = payload.get("invalidation_reference_price")
        target_reference_price = payload.get("target_reference_price")
        risk_reward_ratio = payload.get("risk_reward_ratio")
        signal_id = _escape_html(payload.get("signal_id", "-"))

        header = self.config.paper_mode_prefix if paper_mode else self.config.live_mode_prefix
        probability_pct = f"{probability * 100:.0f}%" if probability <= 1 else f"{probability:.0f}%"

        invalidation_str = (
            _escape_html(invalidation_level) if invalidation_level is not None else "-"
        )
        target_zone_str = _normalize_target_zone(target_zone)

        lines = [
            f"<b>{header} | {symbol}</b>",
            "",
            f"<b>Alert:</b> {alert_type}",
            f"<b>Scenario:</b> {scenario_type}",
            f"<b>Direction:</b> {direction}",
            f"<b>Probability:</b> {probability_pct}",
            f"<b>Market state:</b> {market_state}",
            f"<b>HTF bias:</b> {htf_bias}",
            f"<b>Invalidation:</b> {invalidation_str}",
            f"<b>Target zone:</b> {target_zone_str}",
            f"<b>Cycle:</b> {cycle_id}",
            "",
            f"<b>Execution status:</b> {execution_status}",
            f"<b>Execution model:</b> {execution_model}",
        ]

        if entry_reference_price is not None:
            lines.append(f"<b>Entry:</b> {_escape_html(entry_reference_price)}")
        if invalidation_reference_price is not None:
            lines.append(f"<b>Stop:</b> {_escape_html(invalidation_reference_price)}")
        if target_reference_price is not None:
            lines.append(f"<b>Target:</b> {_escape_html(target_reference_price)}")
        if risk_reward_ratio is not None:
            lines.append(f"<b>RR:</b> {_escape_html(risk_reward_ratio)}")

        lines.extend(
            [
                "",
                f"<b>Reason:</b> {watch_reason}",
                "",
                f"<b>ID:</b> <code>{signal_id}</code>",
            ]
        )

        return _truncate("\n".join(lines), self.config.max_message_length)

    def send_startup_message(self, worker_name: str = "main_worker") -> bool:
        return self.send_text(
            "\n".join(
                [
                    f"<b>🟢 {_escape_html(worker_name)} started</b>",
                    "",
                    "<b>Status:</b> online",
                    "<b>Mode:</b> 24/7 loop",
                ]
            )
        )

    def send_shutdown_message(self, worker_name: str = "main_worker") -> bool:
        return self.send_text(
            "\n".join(
                [
                    f"<b>🛑 {_escape_html(worker_name)} stopped</b>",
                    "",
                    "<b>Status:</b> offline",
                ]
            )
        )

    def send_error_message(self, title: str, details: str) -> bool:
        return self.send_text(
            "\n".join(
                [
                    f"<b>❌ {_escape_html(title)}</b>",
                    "",
                    f"<pre>{_escape_html(_truncate(details, 3000))}</pre>",
                ]
            )
        )


def build_telegram_notifier() -> TelegramNotifier:
    return TelegramNotifier()


def send_alert_payload(payload: Dict[str, Any]) -> bool:
    notifier = build_telegram_notifier()
    return notifier.send_alert_payload(payload)


if __name__ == "__main__":
    sample_payload = {
        "should_alert": True,
        "symbol": "BTCUSD",
        "signal_class": "READY",
        "scenario": "SWEEP_RETURN_LONG",
        "direction": "LONG",
        "confidence": 0.7,
        "rationale": "Sweep and return-to-value setup is fully confirmed.",
        "market_state": "TRANSITION",
        "htf_bias": "SHORT",
        "entry_reference_price": 78000,
        "invalidation_reference_price": 77500,
        "target_reference_price": 79000,
        "execution_status": "EXECUTABLE",
        "execution_model": "LIMIT_ON_RETEST",
        "risk_reward_ratio": 2.0,
        "paper_mode": True,
        "cycle_id": "2026-03-31T10:00:00+00:00",
        "signal_id": "TEST_SIGNAL",
    }

    notifier = build_telegram_notifier()
    ok = notifier.send_alert_payload(sample_payload)
    print(json.dumps({"telegram_sent": ok}, ensure_ascii=False))