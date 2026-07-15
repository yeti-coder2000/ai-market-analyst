from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from app.services.positioning.positioning_feed_builder import write_manual_feed
from app.services.positioning.positioning_store import (
    get_positioning_dir,
    read_json_file,
)


CRYPTO_COLLECTOR_VERSION = "crypto-derivatives-collector-v0.1-offline-snapshot"

DEFAULT_SNAPSHOT_FILENAME = "crypto_derivatives_snapshot.json"

SUPPORTED_SYMBOLS = {
    "BTCUSD": {
        "aliases": {"BTCUSD", "BTCUSDT", "BTC"},
        "default_source": "crypto_derivatives_snapshot_btc",
    },
    "ETHUSD": {
        "aliases": {"ETHUSD", "ETHUSDT", "ETH"},
        "default_source": "crypto_derivatives_snapshot_eth",
    },
}


@dataclass(slots=True)
class CryptoDerivativesInput:
    symbol: str
    price_change_pct: float | None = None

    cme_open_interest_change_pct: float | None = None
    perp_open_interest_change_pct: float | None = None
    open_interest_change_pct: float | None = None

    volume_change_pct_vs_20d: float | None = None
    funding_rate: float | None = None
    funding_rate_pct: float | None = None

    long_liquidations_usd: float | None = None
    short_liquidations_usd: float | None = None

    basis_pct: float | None = None
    source: str | None = None
    source_timestamp: str | None = None
    notes: str | None = None
    flags: list[str] | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "CryptoDerivativesInput":
        return cls(
            symbol=_normalize_symbol(raw.get("symbol")),
            price_change_pct=_to_float(raw.get("price_change_pct")),
            cme_open_interest_change_pct=_to_float(raw.get("cme_open_interest_change_pct")),
            perp_open_interest_change_pct=_to_float(raw.get("perp_open_interest_change_pct")),
            open_interest_change_pct=_to_float(raw.get("open_interest_change_pct")),
            volume_change_pct_vs_20d=_to_float(raw.get("volume_change_pct_vs_20d")),
            funding_rate=_to_float(raw.get("funding_rate")),
            funding_rate_pct=_to_float(raw.get("funding_rate_pct")),
            long_liquidations_usd=_to_float(raw.get("long_liquidations_usd")),
            short_liquidations_usd=_to_float(raw.get("short_liquidations_usd")),
            basis_pct=_to_float(raw.get("basis_pct")),
            source=_clean_str(raw.get("source")) or None,
            source_timestamp=_clean_str(raw.get("source_timestamp")) or None,
            notes=_clean_str(raw.get("notes")) or None,
            flags=_parse_flags(raw.get("flags")),
        )


def build_crypto_feed_items_from_snapshot(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Convert crypto derivatives snapshot into Positioning manual feed items.

    Expected input format:

    {
      "date": "2026-07-15",
      "items": [
        {
          "symbol": "BTCUSD",
          "price_change_pct": -2.4,
          "cme_open_interest_change_pct": -2.1,
          "perp_open_interest_change_pct": 5.8,
          "volume_change_pct_vs_20d": 31.0,
          "funding_rate_pct": 0.034,
          "long_liquidations_usd": 184000000,
          "short_liquidations_usd": 42000000
        }
      ]
    }

    v0.1 outputs only feed items. It does not call exchanges and does not
    modify Battle Gate.
    """

    raw_items = snapshot.get("items") or []
    if not isinstance(raw_items, list):
        return []

    out: list[dict[str, Any]] = []

    for raw in raw_items:
        if not isinstance(raw, dict):
            continue

        parsed = CryptoDerivativesInput.from_dict(raw)
        if parsed.symbol not in SUPPORTED_SYMBOLS:
            continue

        item = _to_feed_item(parsed)
        out.append(item)

    return out


def build_crypto_manual_feed_payload(
    snapshot: dict[str, Any],
    target_date: str | None = None,
) -> dict[str, Any]:
    feed_date = target_date or str(snapshot.get("date") or date.today().isoformat())
    items = build_crypto_feed_items_from_snapshot(snapshot)

    return {
        "version": CRYPTO_COLLECTOR_VERSION,
        "date": feed_date,
        "items": items,
        "collector": {
            "name": "crypto_derivatives_collector",
            "mode": "offline_snapshot",
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
    }


def ensure_sample_crypto_snapshot(
    runtime_dir: str | None = None,
    snapshot_path: str | None = None,
    overwrite: bool = False,
) -> Path:
    path = Path(snapshot_path) if snapshot_path else get_positioning_dir(runtime_dir) / DEFAULT_SNAPSHOT_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and not overwrite:
        return path

    payload = {
        "version": CRYPTO_COLLECTOR_VERSION,
        "date": date.today().isoformat(),
        "generated_at": _utc_now_iso(),
        "items": [
            {
                "symbol": "BTCUSD",
                "price_change_pct": -2.40,
                "cme_open_interest_change_pct": -2.10,
                "perp_open_interest_change_pct": 5.80,
                "volume_change_pct_vs_20d": 31.0,
                "funding_rate_pct": 0.034,
                "long_liquidations_usd": 184000000,
                "short_liquidations_usd": 42000000,
                "basis_pct": 0.42,
                "source": "offline_crypto_derivatives_snapshot",
                "source_timestamp": _utc_now_iso(),
                "notes": "sample BTC derivatives pressure row",
                "flags": ["CRYPTO_EXCHANGE_OI_NOISY"],
            },
            {
                "symbol": "ETHUSD",
                "price_change_pct": 1.15,
                "cme_open_interest_change_pct": 1.20,
                "perp_open_interest_change_pct": 2.40,
                "volume_change_pct_vs_20d": 18.5,
                "funding_rate_pct": 0.018,
                "long_liquidations_usd": 21000000,
                "short_liquidations_usd": 56000000,
                "basis_pct": 0.31,
                "source": "offline_crypto_derivatives_snapshot",
                "source_timestamp": _utc_now_iso(),
                "notes": "sample ETH derivatives pressure row",
                "flags": ["CRYPTO_EXCHANGE_OI_NOISY"],
            },
        ],
    }

    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")

    return path


def load_crypto_snapshot(path: str | Path) -> dict[str, Any]:
    return read_json_file(Path(path))


def _to_feed_item(parsed: CryptoDerivativesInput) -> dict[str, Any]:
    # Prefer CME OI when present. If absent, use generic OI or perp OI,
    # but mark crypto OI quality risk.
    oi_change = (
        parsed.cme_open_interest_change_pct
        if parsed.cme_open_interest_change_pct is not None
        else parsed.open_interest_change_pct
    )

    flags = list(parsed.flags or [])
    _add_flag(flags, "CRYPTO_EXCHANGE_OI_NOISY")

    if parsed.cme_open_interest_change_pct is None and parsed.perp_open_interest_change_pct is not None:
        oi_change = parsed.perp_open_interest_change_pct
        _add_flag(flags, "PERP_OI_PROXY")

    if parsed.funding_rate_pct is not None and abs(parsed.funding_rate_pct) >= 0.03:
        _add_flag(flags, "FUNDING_ELEVATED")

    if _liquidation_pressure(parsed) == "LONG_LIQUIDATION_PRESSURE":
        _add_flag(flags, "LONG_LIQUIDATION_PRESSURE")
    elif _liquidation_pressure(parsed) == "SHORT_LIQUIDATION_PRESSURE":
        _add_flag(flags, "SHORT_LIQUIDATION_PRESSURE")

    notes = _compose_notes(parsed)

    return {
        "symbol": parsed.symbol,
        "price_change_pct": parsed.price_change_pct,
        "volume_change_pct_vs_20d": parsed.volume_change_pct_vs_20d,
        "open_interest_change_pct": oi_change,
        "source": parsed.source or SUPPORTED_SYMBOLS[parsed.symbol]["default_source"],
        "source_timestamp": parsed.source_timestamp,
        "notes": notes,
        "flags": flags,
        "crypto_derivatives": {
            "cme_open_interest_change_pct": parsed.cme_open_interest_change_pct,
            "perp_open_interest_change_pct": parsed.perp_open_interest_change_pct,
            "funding_rate": parsed.funding_rate,
            "funding_rate_pct": parsed.funding_rate_pct,
            "long_liquidations_usd": parsed.long_liquidations_usd,
            "short_liquidations_usd": parsed.short_liquidations_usd,
            "basis_pct": parsed.basis_pct,
            "collector_version": CRYPTO_COLLECTOR_VERSION,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
    }


def _compose_notes(parsed: CryptoDerivativesInput) -> str:
    parts = []
    if parsed.notes:
        parts.append(parsed.notes)

    if parsed.funding_rate_pct is not None:
        parts.append(f"funding_pct={parsed.funding_rate_pct}")

    if parsed.long_liquidations_usd is not None or parsed.short_liquidations_usd is not None:
        parts.append(
            "liquidations_usd="
            f"long:{parsed.long_liquidations_usd or 0:.0f}/"
            f"short:{parsed.short_liquidations_usd or 0:.0f}"
        )

    if parsed.perp_open_interest_change_pct is not None:
        parts.append(f"perp_oi_change_pct={parsed.perp_open_interest_change_pct}")

    if parsed.cme_open_interest_change_pct is not None:
        parts.append(f"cme_oi_change_pct={parsed.cme_open_interest_change_pct}")

    return "; ".join(parts) if parts else "crypto derivatives snapshot"


def _liquidation_pressure(parsed: CryptoDerivativesInput) -> str | None:
    long_liq = parsed.long_liquidations_usd or 0.0
    short_liq = parsed.short_liquidations_usd or 0.0

    if long_liq <= 0 and short_liq <= 0:
        return None

    if long_liq >= max(short_liq * 2.0, 10_000_000):
        return "LONG_LIQUIDATION_PRESSURE"

    if short_liq >= max(long_liq * 2.0, 10_000_000):
        return "SHORT_LIQUIDATION_PRESSURE"

    return None


def _normalize_symbol(value: Any) -> str:
    raw = _clean_str(value).upper().replace("-", "").replace("/", "")
    for canonical, meta in SUPPORTED_SYMBOLS.items():
        if raw in meta["aliases"]:
            return canonical
    return raw


def _to_float(value: Any) -> float | None:
    text = _clean_str(value)
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_flags(value: Any) -> list[str]:
    if isinstance(value, list):
        raw = [str(x) for x in value if x]
    else:
        text = _clean_str(value)
        raw = [x for x in text.replace(",", "|").split("|") if x.strip()] if text else []

    flags: list[str] = []
    for item in raw:
        _add_flag(flags, item.strip().upper())
    return flags


def _add_flag(flags: list[str], flag: str) -> None:
    cleaned = str(flag or "").strip().upper()
    if cleaned and cleaned not in flags:
        flags.append(cleaned)


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build crypto derivatives positioning feed from offline snapshot."
    )
    parser.add_argument("--runtime-dir", default=None, help="Runtime dir.")
    parser.add_argument("--snapshot", default=None, help="Input crypto derivatives snapshot JSON.")
    parser.add_argument("--date", default=None, help="Feed date YYYY-MM-DD.")
    parser.add_argument("--output", default=None, help="Output manual feed path.")
    parser.add_argument("--init-sample", action="store_true", help="Create sample snapshot and exit unless --build-context.")
    parser.add_argument("--overwrite-sample", action="store_true", help="Overwrite sample snapshot.")
    parser.add_argument("--build-context", action="store_true", help="Build daily positioning context after writing feed.")

    args = parser.parse_args()

    snapshot_path = args.snapshot

    if args.init_sample:
        path = ensure_sample_crypto_snapshot(
            runtime_dir=args.runtime_dir,
            snapshot_path=snapshot_path,
            overwrite=args.overwrite_sample,
        )
        print(f"sample_snapshot={path}")
        snapshot_path = str(path)

        if not args.build_context:
            return

    if not snapshot_path:
        snapshot_path = str(get_positioning_dir(args.runtime_dir) / DEFAULT_SNAPSHOT_FILENAME)

    snapshot = load_crypto_snapshot(snapshot_path)
    payload = build_crypto_manual_feed_payload(snapshot, target_date=args.date)
    output_path = write_manual_feed(payload, runtime_dir=args.runtime_dir, output_path=args.output)

    print(f"feed={output_path}")
    print(f"date={payload.get('date')}")
    print(f"items={len(payload.get('items') or [])}")

    if args.build_context:
        from app.services.positioning.positioning_service import build_daily_positioning_context

        context = build_daily_positioning_context(
            runtime_dir=args.runtime_dir,
            feed_path=str(output_path),
            persist=True,
        )
        print(f"context_status={context.get('status')}")
        print(f"context_items={len(context.get('items') or [])}")


if __name__ == "__main__":
    main()
