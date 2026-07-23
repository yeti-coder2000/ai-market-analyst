from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .positioning_models import PositioningFeedItem, PositioningInterpretation


# v0.1 tag set. Research/context only.
FRESH_LONG_PARTICIPATION = "FRESH_LONG_PARTICIPATION"
FRESH_SHORT_PARTICIPATION = "FRESH_SHORT_PARTICIPATION"
SHORT_COVERING_RISK = "SHORT_COVERING_RISK"
LONG_LIQUIDATION_RISK = "LONG_LIQUIDATION_RISK"
OI_SUPPORTS_PRICE_MOVE = "OI_SUPPORTS_PRICE_MOVE"
OI_DIVERGES_FROM_PRICE = "OI_DIVERGES_FROM_PRICE"
LOW_CONVICTION_MOVE = "LOW_CONVICTION_MOVE"
POSITIONING_NEUTRAL = "POSITIONING_NEUTRAL"
DATA_UNAVAILABLE = "DATA_UNAVAILABLE"
DATA_STALE = "DATA_STALE"
PROXY_RISK = "PROXY_RISK"
ROLLOVER_DISTORTION_RISK = "ROLLOVER_DISTORTION_RISK"
CRYPTO_EXCHANGE_OI_NOISY = "CRYPTO_EXCHANGE_OI_NOISY"
VOLUME_CONFIRMS_MOVE = "VOLUME_CONFIRMS_MOVE"
VOLUME_WEAK = "VOLUME_WEAK"
NO_BATTLE_GATE_IMPACT = "NO_BATTLE_GATE_IMPACT"


@dataclass(frozen=True, slots=True)
class PositioningTaggerConfig:
    min_abs_price_change_pct: float = 0.15
    min_abs_oi_change_pct: float = 0.50
    strong_volume_change_pct: float = 15.0
    weak_volume_change_pct: float = -20.0


def _is_up(value: Optional[float], threshold: float) -> bool:
    return value is not None and value >= threshold


def _is_down(value: Optional[float], threshold: float) -> bool:
    return value is not None and value <= -threshold


def _is_flat(value: Optional[float], threshold: float) -> bool:
    return value is not None and abs(value) < threshold


def _bounded_confidence(value: float) -> float:
    return round(max(0.20, min(0.90, value)), 2)


def _contains_any(flags: list[str], needles: tuple[str, ...]) -> bool:
    upper_flags = {str(flag).upper() for flag in flags}
    return any(needle in upper_flags for needle in needles)


def interpret_positioning_item(
    item: PositioningFeedItem,
    config: PositioningTaggerConfig | None = None,
) -> PositioningInterpretation:
    """
    Convert price/OI/volume proxy into a research-only positioning interpretation.

    Important:
    - This is NOT a signal.
    - This must NOT allow or block Battle Gate.
    - It only describes participation quality of the supplied research window.
    """

    cfg = config or PositioningTaggerConfig()
    flags = list(item.flags or [])
    secondary: list[str] = []

    if not item.symbol:
        return PositioningInterpretation(
            primary_tag=DATA_UNAVAILABLE,
            secondary_tags=[NO_BATTLE_GATE_IMPACT],
            confidence=0.20,
            interpretation="Missing symbol. Positioning context unavailable.",
            tpo_note="Ignore positioning layer. Continue using TPO/Auction only.",
            data_quality="BAD",
            flags=["MISSING_SYMBOL", *flags],
        )

    if _contains_any(flags, ("OPERATIONAL_BASELINE_CAPTURED",)):
        return PositioningInterpretation(
            primary_tag=POSITIONING_NEUTRAL,
            secondary_tags=[NO_BATTLE_GATE_IMPACT],
            confidence=0.35,
            interpretation=(
                "Morning absolute price/open-interest baseline captured. "
                "No London-session participation delta exists yet."
            ),
            tpo_note="Baseline only. Do not infer participation direction before the London-close delta.",
            data_quality=(
                "MEDIUM"
                if _contains_any(flags, ("CRYPTO_EXCHANGE_OI_NOISY", "PERP_OI_NOISY"))
                else "GOOD"
            ),
            flags=_dedupe_preserve_order(flags),
        )

    if item.price_change_pct is None or item.open_interest_change_pct is None:
        return PositioningInterpretation(
            primary_tag=DATA_UNAVAILABLE,
            secondary_tags=[NO_BATTLE_GATE_IMPACT],
            confidence=0.25,
            interpretation="Missing price or open interest change. Positioning context unavailable.",
            tpo_note="Ignore positioning layer. Continue using TPO/Auction only.",
            data_quality="BAD",
            flags=["MISSING_PRICE_OR_OI", *flags],
        )

    price_up = _is_up(item.price_change_pct, cfg.min_abs_price_change_pct)
    price_down = _is_down(item.price_change_pct, cfg.min_abs_price_change_pct)
    price_flat = _is_flat(item.price_change_pct, cfg.min_abs_price_change_pct)

    oi_up = _is_up(item.open_interest_change_pct, cfg.min_abs_oi_change_pct)
    oi_down = _is_down(item.open_interest_change_pct, cfg.min_abs_oi_change_pct)
    oi_flat = _is_flat(item.open_interest_change_pct, cfg.min_abs_oi_change_pct)

    if item.volume_change_pct_vs_20d is not None:
        if item.volume_change_pct_vs_20d >= cfg.strong_volume_change_pct:
            secondary.append(VOLUME_CONFIRMS_MOVE)
        elif item.volume_change_pct_vs_20d <= cfg.weak_volume_change_pct:
            secondary.append(VOLUME_WEAK)

    source_upper = (item.source or "").upper()
    if "PROXY" in source_upper or _contains_any(flags, ("PROXY_RISK",)):
        secondary.append(PROXY_RISK)
        flags.append(PROXY_RISK)

    if _contains_any(flags, ("ROLLOVER", "ROLL_OVER")):
        secondary.append(ROLLOVER_DISTORTION_RISK)

    if _contains_any(flags, ("CRYPTO_EXCHANGE_OI_NOISY", "PERP_OI_NOISY")):
        secondary.append(CRYPTO_EXCHANGE_OI_NOISY)

    primary_tag: str
    interpretation: str
    tpo_note: str
    operational_delta = _contains_any(flags, ("OPERATIONAL_DELTA_SINCE_MORNING",))

    if price_up and oi_up:
        primary_tag = FRESH_LONG_PARTICIPATION
        secondary.append(OI_SUPPORTS_PRICE_MOVE)
        interpretation = (
            "Since the morning baseline, price rose with rising open interest. "
            "The London-session move is supported by fresh participation rather than only short covering."
            if operational_delta
            else "Price rose with rising open interest. Previous move is likely supported by fresh participation rather than only short covering."
        )
        tpo_note = (
            "Bullish continuation context improves only if today's auction confirms "
            "acceptance above value/key references. Do not chase first impulse."
        )

    elif price_up and oi_down:
        primary_tag = SHORT_COVERING_RISK
        secondary.append(OI_DIVERGES_FROM_PRICE)
        interpretation = (
            "Since the morning baseline, price rose while open interest declined. "
            "The London-session move may be short covering rather than fresh bullish participation."
            if operational_delta
            else "Price rose while open interest declined. Move may be exit-driven / short covering rather than fresh bullish participation."
        )
        tpo_note = (
            "Do not overrate bullish continuation. Require retest, value acceptance, "
            "and LTF confirmation."
        )

    elif price_down and oi_up:
        primary_tag = FRESH_SHORT_PARTICIPATION
        secondary.append(OI_SUPPORTS_PRICE_MOVE)
        interpretation = (
            "Since the morning baseline, price fell with rising open interest. "
            "The London-session move is supported by fresh bearish participation."
            if operational_delta
            else "Price fell with rising open interest. Previous move is likely supported by fresh bearish participation."
        )
        tpo_note = (
            "Bearish continuation context improves only after clean acceptance below value/key references. "
            "Avoid late chase after the first impulse."
        )

    elif price_down and oi_down:
        primary_tag = LONG_LIQUIDATION_RISK
        secondary.append(OI_DIVERGES_FROM_PRICE)
        interpretation = (
            "Since the morning baseline, price and open interest both fell. "
            "The London-session move may be long liquidation rather than fresh short build."
            if operational_delta
            else "Price fell while open interest declined. Move may be driven by long liquidation or position reduction rather than fresh short build."
        )
        tpo_note = (
            "Continuation can fail after liquidation impulses. Require fresh acceptance/retest before action."
        )

    elif price_flat and oi_flat:
        primary_tag = POSITIONING_NEUTRAL
        interpretation = "Price and open interest were broadly neutral. No clear daily participation signal."
        tpo_note = "Use TPO/Auction as primary context. Positioning layer adds no strong bias."

    else:
        primary_tag = LOW_CONVICTION_MOVE
        interpretation = (
            "Price/OI relationship is mixed or low-conviction. Daily participation quality is unclear."
        )
        tpo_note = (
            "Treat positioning context as weak. Require standard TPO/Auction confirmation."
        )

    secondary = _dedupe_preserve_order([*secondary, NO_BATTLE_GATE_IMPACT])

    confidence = 0.55
    if primary_tag in {
        FRESH_LONG_PARTICIPATION,
        FRESH_SHORT_PARTICIPATION,
        SHORT_COVERING_RISK,
        LONG_LIQUIDATION_RISK,
    }:
        confidence += 0.10
    if VOLUME_CONFIRMS_MOVE in secondary:
        confidence += 0.08
    if VOLUME_WEAK in secondary:
        confidence -= 0.10
    if PROXY_RISK in secondary:
        confidence -= 0.08
    if ROLLOVER_DISTORTION_RISK in secondary:
        confidence -= 0.18
    if primary_tag in {POSITIONING_NEUTRAL, LOW_CONVICTION_MOVE}:
        confidence -= 0.10

    data_quality = "GOOD"
    if ROLLOVER_DISTORTION_RISK in secondary or CRYPTO_EXCHANGE_OI_NOISY in secondary:
        data_quality = "MEDIUM"
    if primary_tag == DATA_UNAVAILABLE:
        data_quality = "BAD"

    return PositioningInterpretation(
        primary_tag=primary_tag,
        secondary_tags=secondary,
        confidence=_bounded_confidence(confidence),
        interpretation=interpretation,
        tpo_note=tpo_note,
        data_quality=data_quality,
        flags=_dedupe_preserve_order(flags),
    )


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not value:
            continue
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out
