from __future__ import annotations

from typing import Any, Iterable


MAX_DEFAULT_ITEMS = 7


def render_positioning_block(
    snapshot: dict[str, Any] | None,
    symbols: Iterable[str] | None = None,
    max_items: int = MAX_DEFAULT_ITEMS,
) -> str:
    """
    Render short Ukrainian briefing block.

    Safe for Telegram briefing.
    This text must be context-only and must never imply trade permission.
    """

    if not snapshot or snapshot.get("status") != "OK":
        return "📊 Positioning Context: дані недоступні."

    items = snapshot.get("items") or []
    if not items:
        return "📊 Positioning Context: дані недоступні."

    symbol_filter = {s.strip().upper() for s in symbols or [] if s}
    if symbol_filter:
        items = [item for item in items if str(item.get("symbol", "")).upper() in symbol_filter]

    if not items:
        return "📊 Positioning Context: для активів у фокусі дані недоступні."

    lines: list[str] = ["📊 Positioning Context"]

    for item in items[:max_items]:
        symbol = str(item.get("symbol", "UNKNOWN")).upper()
        market = item.get("daily_market_data", {}) or {}
        interp = item.get("positioning_interpretation", {}) or {}

        price = _arrow(market.get("price_change_pct"))
        oi = _arrow(market.get("open_interest_change_pct"))
        volume = _arrow(market.get("volume_change_pct_vs_20d"))

        primary_tag = interp.get("primary_tag", "DATA_UNAVAILABLE")
        confidence = interp.get("confidence")
        confidence_text = f" / conf {confidence:.2f}" if isinstance(confidence, (int, float)) else ""

        lines.append("")
        lines.append(f"{symbol}")
        lines.append(f"Daily proxy: Price {price} / OI {oi} / Volume {volume}")
        lines.append(f"Tag: {primary_tag}{confidence_text}")
        lines.append(f"Висновок: {_ua_interpretation(primary_tag)}")
        lines.append(f"Для TPO: {_ua_tpo_note(primary_tag)}")

    lines.append("")
    lines.append("Battle Gate: без змін. Це лише контекст участі, не сигнал.")
    return "\n".join(lines)


def render_compact_positioning_line(item: dict[str, Any] | None) -> str:
    if not item:
        return "Positioning: unavailable"

    symbol = str(item.get("symbol", "UNKNOWN")).upper()
    interp = item.get("positioning_interpretation", {}) or {}
    tag = interp.get("primary_tag", "DATA_UNAVAILABLE")
    confidence = interp.get("confidence")

    if isinstance(confidence, (int, float)):
        return f"Positioning: {symbol} — {tag} / conf {confidence:.2f}"
    return f"Positioning: {symbol} — {tag}"


def _arrow(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "?"

    if number > 0:
        return f"↑ {number:.2f}%"
    if number < 0:
        return f"↓ {abs(number):.2f}%"
    return "→ 0.00%"


def _ua_interpretation(tag: str) -> str:
    mapping = {
        "FRESH_LONG_PARTICIPATION": (
            "рух угору підтриманий зростанням OI; більше схоже на нову участь, "
            "а не лише на short covering."
        ),
        "FRESH_SHORT_PARTICIPATION": (
            "рух униз підтриманий зростанням OI; більше схоже на нову ведмежу участь."
        ),
        "SHORT_COVERING_RISK": (
            "ціна росла, але OI падав; ріст може бути exit-driven / short covering."
        ),
        "LONG_LIQUIDATION_RISK": (
            "ціна падала, але OI падав; рух може бути long liquidation, не обов'язково fresh short build."
        ),
        "LOW_CONVICTION_MOVE": (
            "зв'язка price/OI/volume змішана; якість участі неясна."
        ),
        "POSITIONING_NEUTRAL": (
            "немає сильного позиційного перекосу за daily proxy."
        ),
        "DATA_UNAVAILABLE": (
            "даних недостатньо для позиційного висновку."
        ),
    }
    return mapping.get(tag, "позиційний контекст потребує додаткової перевірки.")


def _ua_tpo_note(tag: str) -> str:
    mapping = {
        "FRESH_LONG_PARTICIPATION": (
            "continuation long має сенс тільки після clean acceptance вище value / ключової зони. "
            "Chase першого імпульсу заборонений."
        ),
        "FRESH_SHORT_PARTICIPATION": (
            "continuation short має сенс тільки після clean acceptance нижче value / ключової зони. "
            "Не продавати пізній імпульс без retest."
        ),
        "SHORT_COVERING_RISK": (
            "не переоцінювати bullish continuation; потрібні retest, acceptance і LTF confirmation."
        ),
        "LONG_LIQUIDATION_RISK": (
            "після liquidation impulse continuation може швидко згаснути; чекати fresh acceptance / retest."
        ),
        "LOW_CONVICTION_MOVE": (
            "позиційний шар слабкий; головна логіка — TPO / Auction."
        ),
        "POSITIONING_NEUTRAL": (
            "позиційний шар не додає bias; працюємо по стандартній TPO-логіці."
        ),
        "DATA_UNAVAILABLE": (
            "ігнорувати positioning layer; TPO / Auction лишається головним джерелом контексту."
        ),
    }
    return mapping.get(tag, "використовувати тільки як context-only, без впливу на Battle Gate.")
