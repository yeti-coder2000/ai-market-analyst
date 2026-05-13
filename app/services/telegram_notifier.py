from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib import error, parse, request


logger = logging.getLogger(__name__)


# =============================================================================
# TELEGRAM NOTIFIER CONFIG / HELPERS
# =============================================================================

MIN_STOP_DISTANCE_BY_SYMBOL: dict[str, float] = {
    "XAUUSD": 15.0,
    "BTCUSD": 100.0,
    "ETHUSD": 8.0,
    "EURUSD": 0.0005,
    "GBPUSD": 0.0007,
    "AUDUSD": 0.0005,
    "USDJPY": 0.08,
    "USDCHF": 0.0005,
    "USDCAD": 0.0007,
    "GER40": 25.0,
    "NAS100": 35.0,
    "SPX500": 8.0,
    "UKOIL": 0.25,
}


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


def _safe_float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if value == "":
            continue
        return value
    return None


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


def _normalize_direction(value: Any) -> str:
    direction = str(value or "NEUTRAL").strip().upper()
    if direction in {"LONG", "SHORT", "NEUTRAL"}:
        return direction
    return "NEUTRAL"


def _normalize_htf_bias(value: Any) -> str:
    htf_bias = str(value or "NEUTRAL").strip().upper()
    if htf_bias in {"LONG", "SHORT", "NEUTRAL"}:
        return htf_bias
    return "NEUTRAL"


def _derive_signal_alignment(direction: Any, htf_bias: Any) -> tuple[str, str, str]:
    d = _normalize_direction(direction)
    h = _normalize_htf_bias(htf_bias)

    if d not in {"LONG", "SHORT"}:
        return "NO_DIRECTION", "⚫", "NO DIRECTION"

    if h == "NEUTRAL":
        return "NEUTRAL_HTF", "⚪", "NEUTRAL HTF"

    if h not in {"LONG", "SHORT"}:
        return "UNKNOWN_HTF", "⚫", "UNKNOWN HTF"

    if d == h:
        return "TREND_ALIGNED", "🟢", "TREND-ALIGNED"

    return "COUNTER_TREND", "🔴", "COUNTER-TREND"


def _derive_stop_quality(
    *,
    symbol: Any,
    entry: Any,
    stop: Any,
    target: Any,
    rr: Any,
) -> tuple[str, str, float | None, float | None]:
    """
    Returns:
    - stop_quality
    - stop_quality_reason
    - theoretical_rr
    - practical_rr
    """
    symbol_text = str(symbol or "").strip().upper()
    entry_f = _safe_float_or_none(entry)
    stop_f = _safe_float_or_none(stop)
    target_f = _safe_float_or_none(target)
    rr_f = _safe_float_or_none(rr)

    theoretical_rr = rr_f

    if entry_f is None or stop_f is None or target_f is None:
        return "UNKNOWN", "missing entry/stop/target", theoretical_rr, None

    stop_distance = abs(entry_f - stop_f)
    target_distance = abs(target_f - entry_f)

    if stop_distance <= 0:
        return "INVALID", "stop distance is zero or negative", theoretical_rr, None

    min_stop = MIN_STOP_DISTANCE_BY_SYMBOL.get(symbol_text)

    if min_stop is None:
        return (
            "OK",
            "no instrument-specific practical stop threshold",
            theoretical_rr,
            theoretical_rr,
        )

    if stop_distance < min_stop:
        practical_rr = round(target_distance / min_stop, 3) if min_stop > 0 else None
        return (
            "TIGHT_STOP",
            f"stop_distance {stop_distance:.5f} below practical_min_stop {min_stop:.5f}",
            theoretical_rr,
            practical_rr,
        )

    return (
        "OK",
        f"stop_distance {stop_distance:.5f} >= practical_min_stop {min_stop:.5f}",
        theoretical_rr,
        theoretical_rr,
    )


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
    Also adds derived risk labels:
    - signal_alignment
    - stop_quality
    - theoretical_rr
    - practical_rr
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

    direction = normalized.get("direction")
    htf_bias = normalized.get("htf_bias")

    signal_alignment, signal_alignment_marker, signal_alignment_label = _derive_signal_alignment(
        direction,
        htf_bias,
    )

    normalized.setdefault("signal_alignment", signal_alignment)
    normalized.setdefault("signal_alignment_marker", signal_alignment_marker)
    normalized.setdefault("signal_alignment_label", signal_alignment_label)

    entry = _first_present(
        normalized.get("entry_reference_price"),
        normalized.get("entry"),
    )
    stop = _first_present(
        normalized.get("invalidation_reference_price"),
        normalized.get("stop_loss"),
        normalized.get("stop"),
    )
    target = _first_present(
        normalized.get("target_reference_price"),
        normalized.get("take_profit"),
        normalized.get("target"),
    )
    rr = _first_present(
        normalized.get("risk_reward_ratio"),
        normalized.get("rr"),
        normalized.get("risk_reward"),
    )

    stop_quality, stop_quality_reason, theoretical_rr, practical_rr = _derive_stop_quality(
        symbol=normalized.get("symbol"),
        entry=entry,
        stop=stop,
        target=target,
        rr=rr,
    )

    normalized.setdefault("stop_quality", stop_quality)
    normalized.setdefault("stop_quality_reason", stop_quality_reason)
    normalized.setdefault("theoretical_rr", theoretical_rr)
    normalized.setdefault("practical_rr", practical_rr)

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
            "Sending Telegram alert. symbol=%s alert_type=%s signal_id=%s alignment=%s stop_quality=%s practical_rr=%s",
            normalized_payload.get("symbol"),
            alert_type,
            normalized_payload.get("signal_id"),
            normalized_payload.get("signal_alignment"),
            normalized_payload.get("stop_quality"),
            normalized_payload.get("practical_rr"),
        )
        return self.send_text(message)

    def format_alert_payload(self, payload: Dict[str, Any]) -> str:
        payload = _normalize_alert_payload(payload)

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

        signal_alignment = str(payload.get("signal_alignment") or "UNKNOWN")
        signal_alignment_marker = str(payload.get("signal_alignment_marker") or "⚫")
        signal_alignment_label = str(payload.get("signal_alignment_label") or signal_alignment)

        stop_quality = str(payload.get("stop_quality") or "UNKNOWN")
        stop_quality_reason = str(payload.get("stop_quality_reason") or "")
        theoretical_rr = payload.get("theoretical_rr")
        practical_rr = payload.get("practical_rr")

        header = self.config.paper_mode_prefix if paper_mode else self.config.live_mode_prefix
        probability_pct = f"{probability * 100:.0f}%" if probability <= 1 else f"{probability:.0f}%"

        invalidation_str = (
            _escape_html(invalidation_level) if invalidation_level is not None else "-"
        )
        target_zone_str = _normalize_target_zone(target_zone)

        lines = [
            f"<b>{header} | {symbol}</b>",
            f"<b>{_escape_html(signal_alignment_marker)} {_escape_html(signal_alignment_label)}</b>",
        ]

        if stop_quality == "TIGHT_STOP":
            lines.append("<b>⚠️ TIGHT STOP / RR INFLATED</b>")
        elif stop_quality == "INVALID":
            lines.append("<b>⛔ INVALID STOP GEOMETRY</b>")

        lines.extend(
            [
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
        )

        if entry_reference_price is not None:
            lines.append(f"<b>Entry:</b> {_escape_html(entry_reference_price)}")
        if invalidation_reference_price is not None:
            lines.append(f"<b>Stop:</b> {_escape_html(invalidation_reference_price)}")
        if target_reference_price is not None:
            lines.append(f"<b>Target:</b> {_escape_html(target_reference_price)}")
        if risk_reward_ratio is not None:
            lines.append(f"<b>RR:</b> {_escape_html(risk_reward_ratio)}")

        if practical_rr is not None and theoretical_rr is not None:
            try:
                practical_rr_f = float(practical_rr)
                theoretical_rr_f = float(theoretical_rr)

                if abs(practical_rr_f - theoretical_rr_f) >= 0.05:
                    lines.append(f"<b>Practical RR:</b> {_escape_html(f'{practical_rr_f:.2f}')}")
            except (TypeError, ValueError):
                pass
        elif practical_rr is not None:
            try:
                lines.append(f"<b>Practical RR:</b> {_escape_html(f'{float(practical_rr):.2f}')}")
            except (TypeError, ValueError):
                lines.append(f"<b>Practical RR:</b> {_escape_html(practical_rr)}")

        if stop_quality in {"TIGHT_STOP", "INVALID"} and stop_quality_reason:
            lines.append(f"<b>Stop quality:</b> {_escape_html(stop_quality)}")
            lines.append(f"<b>Stop note:</b> {_escape_html(stop_quality_reason)}")

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