from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


POSITIONING_LAYER_VERSION = "positioning-intelligence-v0.1-research-only"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_symbol(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


@dataclass(slots=True)
class PositioningFeedItem:
    """
    Raw/manual daily positioning proxy item.

    This is intentionally generic. In v0.1 the source can be manual,
    later collectors can map CME/crypto/ETF/options data into this shape.
    """

    symbol: str
    price_change_pct: Optional[float] = None
    volume_change_pct_vs_20d: Optional[float] = None
    open_interest_change_pct: Optional[float] = None

    price: Optional[float] = None
    volume: Optional[float] = None
    open_interest: Optional[float] = None

    source: str = "manual"
    source_timestamp: Optional[str] = None
    notes: Optional[str] = None
    flags: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PositioningFeedItem":
        return cls(
            symbol=_clean_symbol(raw.get("symbol")),
            price_change_pct=_to_float(raw.get("price_change_pct")),
            volume_change_pct_vs_20d=_to_float(raw.get("volume_change_pct_vs_20d")),
            open_interest_change_pct=_to_float(raw.get("open_interest_change_pct")),
            price=_to_float(raw.get("price")),
            volume=_to_float(raw.get("volume")),
            open_interest=_to_float(raw.get("open_interest")),
            source=str(raw.get("source") or "manual").strip(),
            source_timestamp=raw.get("source_timestamp"),
            notes=raw.get("notes"),
            flags=[str(x) for x in raw.get("flags", []) if x],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "price_change_pct": self.price_change_pct,
            "volume_change_pct_vs_20d": self.volume_change_pct_vs_20d,
            "open_interest_change_pct": self.open_interest_change_pct,
            "price": self.price,
            "volume": self.volume,
            "open_interest": self.open_interest,
            "source": self.source,
            "source_timestamp": self.source_timestamp,
            "notes": self.notes,
            "flags": self.flags,
        }


@dataclass(slots=True)
class PositioningInterpretation:
    primary_tag: str
    secondary_tags: list[str]
    confidence: float
    interpretation: str
    tpo_note: str
    data_quality: str = "GOOD"
    flags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "primary_tag": self.primary_tag,
            "secondary_tags": self.secondary_tags,
            "confidence": self.confidence,
            "interpretation": self.interpretation,
            "tpo_note": self.tpo_note,
            "data_quality": self.data_quality,
            "flags": self.flags,
        }


@dataclass(slots=True)
class PositioningContextItem:
    date: str
    symbol: str
    market_proxy: dict[str, Any]
    daily_market_data: dict[str, Any]
    positioning_interpretation: PositioningInterpretation
    auction_usage: dict[str, Any]
    data_quality: dict[str, Any]
    raw_source: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "symbol": self.symbol,
            "market_proxy": self.market_proxy,
            "daily_market_data": self.daily_market_data,
            "positioning_interpretation": self.positioning_interpretation.to_dict(),
            "auction_usage": self.auction_usage,
            "data_quality": self.data_quality,
            "raw_source": self.raw_source,
        }


@dataclass(slots=True)
class PositioningSnapshot:
    version: str
    generated_at: str
    date: str
    status: str
    items: list[PositioningContextItem]
    source_health: dict[str, Any]
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "date": self.date,
            "status": self.status,
            "items": [item.to_dict() for item in self.items],
            "source_health": self.source_health,
            "warnings": self.warnings,
        }
