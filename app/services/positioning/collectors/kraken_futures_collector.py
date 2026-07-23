from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

from app.services.positioning.positioning_store import (
    get_positioning_dir,
    write_json_atomic,
)


KRAKEN_FUTURES_COLLECTOR_VERSION = "kraken-futures-collector-v0.1-public-tickers"
DEFAULT_KRAKEN_FUTURES_BASE_URL = "https://futures.kraken.com/derivatives/api/v3"
DEFAULT_KRAKEN_SNAPSHOT_FILENAME = "kraken_futures_positioning_snapshot.json"

KRAKEN_FUTURES_SYMBOLS: dict[str, tuple[str, ...]] = {
    "BTCUSD": ("PF_BTCUSD", "PF_XBTUSD", "PI_XBTUSD"),
    "ETHUSD": ("PF_ETHUSD", "PI_ETHUSD"),
}


@dataclass(frozen=True, slots=True)
class KrakenFuturesCollectorConfig:
    base_url: str = DEFAULT_KRAKEN_FUTURES_BASE_URL
    timeout_sec: float = 10.0

    @classmethod
    def from_env(cls) -> "KrakenFuturesCollectorConfig":
        return cls(
            base_url=str(
                os.getenv("KRAKEN_FUTURES_BASE_URL")
                or DEFAULT_KRAKEN_FUTURES_BASE_URL
            ).rstrip("/"),
            timeout_sec=_env_float("POSITIONING_KRAKEN_TIMEOUT_SEC", 10.0, minimum=1.0),
        )


def collect_kraken_futures_snapshot(
    *,
    target_date: str | None = None,
    symbols: Iterable[str] | None = None,
    config: KrakenFuturesCollectorConfig | None = None,
    session: Any | None = None,
) -> dict[str, Any]:
    """Collect a public BTC/ETH derivatives proxy without account credentials."""

    cfg = config or KrakenFuturesCollectorConfig.from_env()
    requested = _normalize_requested_symbols(symbols)
    owns_session = session is None
    http = session or requests.Session()
    url = f"{cfg.base_url}/tickers"

    try:
        response = http.get(
            url,
            timeout=cfg.timeout_sec,
            headers={
                "Accept": "application/json",
                "User-Agent": "ai-market-analyst-positioning/0.4",
            },
        )
        response.raise_for_status()
        payload = response.json()
    finally:
        if owns_session:
            close = getattr(http, "close", None)
            if callable(close):
                close()

    if not isinstance(payload, dict) or str(payload.get("result") or "").lower() != "success":
        raise ValueError("Kraken Futures tickers response is not successful")

    tickers = payload.get("tickers")
    if not isinstance(tickers, list):
        raise ValueError("Kraken Futures tickers response has no ticker list")

    server_time = str(payload.get("serverTime") or _utc_now_iso())
    items: list[dict[str, Any]] = []
    health: list[dict[str, Any]] = []
    warnings: list[str] = []
    errors: list[str] = []

    for canonical in requested:
        ticker = _select_ticker(tickers, canonical)
        if ticker is None:
            warnings.append(f"kraken_perpetual_missing:{canonical}")
            health.append({"symbol": canonical, "status": "MISSING"})
            continue

        try:
            item = _ticker_to_item(canonical, ticker, server_time)
        except Exception as exc:  # noqa: BLE001
            message = f"{canonical}:{type(exc).__name__}:{exc}"
            errors.append(message)
            health.append(
                {
                    "symbol": canonical,
                    "exchange_symbol": ticker.get("symbol"),
                    "status": "ERROR",
                    "error": message,
                }
            )
            continue
        items.append(item)
        if "STALE_SOURCE_DATA" in item.get("flags", []):
            warnings.append(f"kraken_source_stale:{canonical}")
        health.append(
            {
                "symbol": canonical,
                "exchange_symbol": ticker.get("symbol"),
                "status": "OK",
                "source_timestamp": item.get("source_timestamp"),
            }
        )

    if len(items) == len(requested) and not errors and not warnings:
        status = "OK"
    elif items:
        status = "PARTIAL"
    else:
        status = "ERROR"

    return {
        "version": KRAKEN_FUTURES_COLLECTOR_VERSION,
        "date": str(target_date or date.today().isoformat()),
        "generated_at": _utc_now_iso(),
        "items": items,
        "collector": {
            "name": "kraken_futures_public_tickers",
            "mode": "live_public_rest_fallback",
            "status": status,
            "base_url": cfg.base_url,
            "symbols_requested": requested,
            "symbols_collected": [item["symbol"] for item in items],
            "symbol_health": health,
            "warnings": warnings,
            "errors": errors,
            "authentication": "none_public_market_data",
            "operational_delta_source": "morning_baseline_persistence",
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }


def collect_and_write_kraken_futures_snapshot(
    *,
    runtime_dir: str | None = None,
    output_path: str | None = None,
    target_date: str | None = None,
    symbols: Iterable[str] | None = None,
    config: KrakenFuturesCollectorConfig | None = None,
    session: Any | None = None,
    persist_empty: bool = False,
) -> tuple[Path, dict[str, Any]]:
    payload = collect_kraken_futures_snapshot(
        target_date=target_date,
        symbols=symbols,
        config=config,
        session=session,
    )
    path = (
        Path(output_path)
        if output_path
        else get_positioning_dir(runtime_dir) / DEFAULT_KRAKEN_SNAPSHOT_FILENAME
    )
    if payload.get("items") or persist_empty:
        write_json_atomic(path, payload)
    return path, payload


def _select_ticker(tickers: list[Any], canonical: str) -> dict[str, Any] | None:
    rows = [row for row in tickers if isinstance(row, dict)]
    by_symbol = {str(row.get("symbol") or "").upper(): row for row in rows}
    for candidate in KRAKEN_FUTURES_SYMBOLS[canonical]:
        if candidate in by_symbol:
            return by_symbol[candidate]

    token = "ETH" if canonical == "ETHUSD" else ("XBT", "BTC")
    tokens = (token,) if isinstance(token, str) else token
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        pair = str(row.get("pair") or "").upper()
        tag = str(row.get("tag") or "").lower()
        if tag != "perpetual" and not symbol.startswith(("PF_", "PI_")):
            continue
        if any(value in symbol or value in pair for value in tokens):
            return row
    return None


def _ticker_to_item(canonical: str, ticker: dict[str, Any], server_time: str) -> dict[str, Any]:
    if bool(ticker.get("suspended")):
        raise ValueError(f"Kraken ticker is suspended for {canonical}")

    price = _first_float(ticker, "last", "markPrice", "indexPrice")
    open_interest = _first_float(ticker, "openInterest")
    if price is None or price <= 0:
        raise ValueError(f"Kraken ticker missing price for {canonical}")
    if open_interest is None or open_interest <= 0:
        raise ValueError(f"Kraken ticker missing open interest for {canonical}")

    mark_price = _first_float(ticker, "markPrice")
    index_price = _first_float(ticker, "indexPrice")
    funding_rate = _first_float(ticker, "fundingRate")
    basis_pct = (
        ((mark_price - index_price) / abs(index_price)) * 100.0
        if mark_price is not None and index_price not in {None, 0.0}
        else None
    )
    source_timestamp = str(ticker.get("lastTime") or server_time or _utc_now_iso())
    source_age_minutes = _timestamp_age_minutes(source_timestamp, server_time)
    flags = [
        "KRAKEN_FUTURES_PUBLIC_DATA",
        "PERP_OI_PROXY",
        "CRYPTO_EXCHANGE_OI_NOISY",
        "LIQUIDATION_AGGREGATE_UNAVAILABLE",
    ]
    if (
        source_age_minutes is not None
        and source_age_minutes > _env_float("POSITIONING_KRAKEN_STALE_MINUTES", 60.0, minimum=1.0)
    ):
        flags.append("STALE_SOURCE_DATA")

    return {
        "symbol": canonical,
        "price_change_pct": _round_optional(_first_float(ticker, "change24h")),
        "volume_change_pct_vs_20d": None,
        "perp_open_interest_change_pct": None,
        "open_interest_change_pct": None,
        "price": price,
        "volume": _first_float(ticker, "volumeQuote", "vol24h"),
        "open_interest": open_interest,
        "funding_rate": funding_rate,
        "funding_rate_pct": _round_optional(funding_rate * 100.0 if funding_rate is not None else None),
        "long_liquidations_usd": None,
        "short_liquidations_usd": None,
        "basis_pct": _round_optional(basis_pct),
        "source": f"kraken_futures_public_{str(ticker.get('symbol') or canonical).lower()}",
        "source_timestamp": source_timestamp,
        "notes": "Kraken Futures public OI proxy; intraday change requires persisted morning baseline.",
        "flags": flags,
        "kraken_futures": {
            "exchange_symbol": ticker.get("symbol"),
            "pair": ticker.get("pair"),
            "tag": ticker.get("tag"),
            "mark_price": mark_price,
            "index_price": index_price,
            "source_age_minutes": source_age_minutes,
            "collector_version": KRAKEN_FUTURES_COLLECTOR_VERSION,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
    }


def _normalize_requested_symbols(symbols: Iterable[str] | None) -> list[str]:
    values = list(symbols) if symbols is not None else list(KRAKEN_FUTURES_SYMBOLS)
    out: list[str] = []
    for value in values:
        raw = str(value or "").upper().replace("-", "").replace("/", "")
        if raw in {"BTC", "XBT", "BTCUSD", "XBTUSD"}:
            canonical = "BTCUSD"
        elif raw in {"ETH", "ETHUSD"}:
            canonical = "ETHUSD"
        else:
            canonical = raw
        if canonical not in KRAKEN_FUTURES_SYMBOLS:
            raise ValueError(f"unsupported Kraken Futures positioning symbol: {value}")
        if canonical not in out:
            out.append(canonical)
    if not out:
        raise ValueError("at least one Kraken Futures symbol is required")
    return out


def _first_float(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = payload.get(key)
        if value in {None, ""}:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _round_optional(value: float | None, digits: int = 6) -> float | None:
    return round(value, digits) if value is not None else None


def _timestamp_age_minutes(source_timestamp: str, server_time: str) -> float | None:
    try:
        source_dt = datetime.fromisoformat(str(source_timestamp).replace("Z", "+00:00"))
        server_dt = datetime.fromisoformat(str(server_time).replace("Z", "+00:00"))
        if source_dt.tzinfo is None:
            source_dt = source_dt.replace(tzinfo=timezone.utc)
        if server_dt.tzinfo is None:
            server_dt = server_dt.replace(tzinfo=timezone.utc)
        return round(max(0.0, (server_dt - source_dt).total_seconds() / 60.0), 3)
    except (TypeError, ValueError):
        return None


def _env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    try:
        value = float(str(raw).strip()) if raw not in {None, ""} else default
    except ValueError:
        value = default
    return max(minimum, value) if minimum is not None else value


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect public Kraken Futures positioning proxy.")
    parser.add_argument("--runtime-dir", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--date", default=None)
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--persist-empty", action="store_true")
    args = parser.parse_args()

    path, payload = collect_and_write_kraken_futures_snapshot(
        runtime_dir=args.runtime_dir,
        output_path=args.output,
        target_date=args.date,
        symbols=args.symbol or None,
        persist_empty=args.persist_empty,
    )
    print(json.dumps({"snapshot": str(path), **payload}, ensure_ascii=False, indent=2))
    return 0 if payload.get("items") else 1


if __name__ == "__main__":
    raise SystemExit(main())
