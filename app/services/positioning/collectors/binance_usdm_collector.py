from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

from app.services.positioning.collectors.crypto_derivatives_collector import (
    DEFAULT_SNAPSHOT_FILENAME,
)
from app.services.positioning.positioning_store import (
    get_positioning_dir,
    write_json_atomic,
)


BINANCE_USDM_COLLECTOR_VERSION = "binance-usdm-collector-v0.2-public-rest"
DEFAULT_BINANCE_USDM_BASE_URL = "https://fapi.binance.com"
DEFAULT_TIMEOUT_SEC = 10.0
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_SEC = 0.75
DEFAULT_VOLUME_LOOKBACK_DAYS = 20
DEFAULT_OI_HISTORY_HOURS = 25

BINANCE_USDM_SYMBOLS: dict[str, str] = {
    "BTCUSD": "BTCUSDT",
    "ETHUSD": "ETHUSDT",
}


@dataclass(frozen=True, slots=True)
class BinanceUsdmCollectorConfig:
    base_url: str = DEFAULT_BINANCE_USDM_BASE_URL
    timeout_sec: float = DEFAULT_TIMEOUT_SEC
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    backoff_sec: float = DEFAULT_BACKOFF_SEC
    volume_lookback_days: int = DEFAULT_VOLUME_LOOKBACK_DAYS
    oi_history_hours: int = DEFAULT_OI_HISTORY_HOURS

    @classmethod
    def from_env(cls) -> "BinanceUsdmCollectorConfig":
        return cls(
            base_url=str(
                os.getenv("BINANCE_USDM_BASE_URL")
                or DEFAULT_BINANCE_USDM_BASE_URL
            ).rstrip("/"),
            timeout_sec=_env_float(
                "POSITIONING_BINANCE_TIMEOUT_SEC",
                DEFAULT_TIMEOUT_SEC,
                minimum=1.0,
            ),
            max_attempts=_env_int(
                "POSITIONING_BINANCE_MAX_ATTEMPTS",
                DEFAULT_MAX_ATTEMPTS,
                minimum=1,
                maximum=5,
            ),
            backoff_sec=_env_float(
                "POSITIONING_BINANCE_BACKOFF_SEC",
                DEFAULT_BACKOFF_SEC,
                minimum=0.0,
            ),
            volume_lookback_days=_env_int(
                "POSITIONING_BINANCE_VOLUME_LOOKBACK_DAYS",
                DEFAULT_VOLUME_LOOKBACK_DAYS,
                minimum=5,
                maximum=60,
            ),
            oi_history_hours=_env_int(
                "POSITIONING_BINANCE_OI_HISTORY_HOURS",
                DEFAULT_OI_HISTORY_HOURS,
                minimum=2,
                maximum=30,
            ),
        )


def collect_binance_usdm_snapshot(
    *,
    target_date: str | None = None,
    symbols: Iterable[str] | None = None,
    config: BinanceUsdmCollectorConfig | None = None,
    session: Any | None = None,
) -> dict[str, Any]:
    """
    Collect BTC/ETH USD-M perpetual participation data from Binance public REST.

    Research-only guarantees:
    - no API key or account endpoint;
    - no order placement;
    - no Battle Gate or Telegram signal permission impact;
    - per-symbol failures are isolated and returned as source health.
    """

    cfg = config or BinanceUsdmCollectorConfig.from_env()
    requested = _normalize_requested_symbols(symbols)
    owns_session = session is None
    http = session or requests.Session()

    items: list[dict[str, Any]] = []
    symbol_health: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []

    try:
        for canonical in requested:
            exchange_symbol = BINANCE_USDM_SYMBOLS[canonical]
            try:
                item, health = _collect_symbol(
                    canonical_symbol=canonical,
                    exchange_symbol=exchange_symbol,
                    config=cfg,
                    session=http,
                )
                items.append(item)
                symbol_health.append(health)
                warnings.extend(str(value) for value in health.get("warnings") or [])
            except Exception as exc:  # noqa: BLE001
                message = f"{canonical}:{type(exc).__name__}:{exc}"
                errors.append(message)
                symbol_health.append(
                    {
                        "symbol": canonical,
                        "exchange_symbol": exchange_symbol,
                        "status": "ERROR",
                        "error": message,
                    }
                )
    finally:
        if owns_session:
            close = getattr(http, "close", None)
            if callable(close):
                close()

    if len(items) == len(requested) and not errors and not warnings:
        status = "OK"
    elif items:
        status = "PARTIAL"
    else:
        status = "ERROR"

    generated_at = _utc_now_iso()
    return {
        "version": BINANCE_USDM_COLLECTOR_VERSION,
        "date": str(target_date or date.today().isoformat()),
        "generated_at": generated_at,
        "items": items,
        "collector": {
            "name": "binance_usdm_public_collector",
            "mode": "live_public_rest",
            "status": status,
            "base_url": cfg.base_url,
            "symbols_requested": requested,
            "symbols_collected": [str(item.get("symbol")) for item in items],
            "symbol_health": symbol_health,
            "errors": errors,
            "warnings": warnings,
            "authentication": "none_public_market_data",
            "liquidation_aggregate": "not_collected_public_rest_unavailable",
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
    }


def collect_and_write_binance_usdm_snapshot(
    *,
    runtime_dir: str | None = None,
    output_path: str | None = None,
    target_date: str | None = None,
    symbols: Iterable[str] | None = None,
    config: BinanceUsdmCollectorConfig | None = None,
    session: Any | None = None,
    persist_empty: bool = False,
) -> tuple[Path, dict[str, Any]]:
    """
    Collect and atomically persist the snapshot.

    A zero-item failed refresh does not overwrite the last usable snapshot by
    default. This preserves fail-open behavior when Binance is temporarily
    unavailable.
    """

    payload = collect_binance_usdm_snapshot(
        target_date=target_date,
        symbols=symbols,
        config=config,
        session=session,
    )
    path = (
        Path(output_path)
        if output_path
        else get_positioning_dir(runtime_dir) / DEFAULT_SNAPSHOT_FILENAME
    )

    if payload.get("items") or persist_empty:
        write_json_atomic(path, payload)

    return path, payload


def _collect_symbol(
    *,
    canonical_symbol: str,
    exchange_symbol: str,
    config: BinanceUsdmCollectorConfig,
    session: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    ticker = _get_json(
        session=session,
        config=config,
        path="/fapi/v1/ticker/24hr",
        params={"symbol": exchange_symbol},
    )
    endpoint_warnings: list[str] = []
    premium = _get_json_optional(
        session=session,
        config=config,
        path="/fapi/v1/premiumIndex",
        params={"symbol": exchange_symbol},
        warnings=endpoint_warnings,
    )
    current_oi = _get_json(
        session=session,
        config=config,
        path="/fapi/v1/openInterest",
        params={"symbol": exchange_symbol},
    )
    oi_history = _get_json(
        session=session,
        config=config,
        path="/futures/data/openInterestHist",
        params={
            "symbol": exchange_symbol,
            "period": "1h",
            "limit": config.oi_history_hours,
        },
    )
    klines = _get_json_optional(
        session=session,
        config=config,
        path="/fapi/v1/klines",
        params={
            "symbol": exchange_symbol,
            "interval": "1d",
            "limit": config.volume_lookback_days + 1,
        },
        warnings=endpoint_warnings,
    )

    if not isinstance(ticker, dict):
        raise ValueError("ticker response must be an object")
    if not isinstance(premium, dict):
        premium = {}
    if not isinstance(current_oi, dict):
        raise ValueError("open interest response must be an object")
    if not isinstance(oi_history, list):
        raise ValueError("open interest history response must be a list")
    if not isinstance(klines, list):
        klines = []

    price_change_pct = _required_float(
        ticker.get("priceChangePercent"),
        "ticker.priceChangePercent",
    )
    price = _required_float(ticker.get("lastPrice"), "ticker.lastPrice")
    rolling_quote_volume = _to_float(ticker.get("quoteVolume"))

    current_open_interest = _required_float(
        current_oi.get("openInterest"),
        "openInterest.openInterest",
    )
    baseline_open_interest, baseline_timestamp_ms = _oi_baseline(oi_history)
    oi_change_pct = _pct_change(current_open_interest, baseline_open_interest)

    funding_rate = _to_float(premium.get("lastFundingRate"))
    funding_rate_pct = funding_rate * 100.0 if funding_rate is not None else None
    mark_price = _to_float(premium.get("markPrice"))
    index_price = _to_float(premium.get("indexPrice"))
    basis_pct = (
        _pct_change(mark_price, index_price)
        if mark_price is not None and index_price not in {None, 0.0}
        else None
    )

    volume_change_pct_vs_20d = _volume_change_vs_closed_daily_average(
        rolling_quote_volume=rolling_quote_volume,
        klines=klines,
        lookback_days=config.volume_lookback_days,
    )

    source_timestamp_ms = max(
        _to_int(ticker.get("closeTime")) or 0,
        _to_int(premium.get("time")) or 0,
        _to_int(current_oi.get("time")) or 0,
        baseline_timestamp_ms or 0,
    )
    source_timestamp = (
        _millis_to_iso(source_timestamp_ms)
        if source_timestamp_ms > 0
        else _utc_now_iso()
    )

    flags = [
        "BINANCE_USDM_PUBLIC_DATA",
        "PERP_OI_PROXY",
        "CRYPTO_EXCHANGE_OI_NOISY",
        "LIQUIDATION_AGGREGATE_UNAVAILABLE",
    ]
    if funding_rate_pct is not None and abs(funding_rate_pct) >= 0.03:
        flags.append("FUNDING_ELEVATED")
    if volume_change_pct_vs_20d is None:
        flags.append("VOLUME_BASELINE_UNAVAILABLE")

    item = {
        "symbol": canonical_symbol,
        "price_change_pct": round(price_change_pct, 6),
        "volume_change_pct_vs_20d": _round_optional(volume_change_pct_vs_20d),
        "perp_open_interest_change_pct": round(oi_change_pct, 6),
        "open_interest_change_pct": round(oi_change_pct, 6),
        "price": price,
        "volume": rolling_quote_volume,
        "open_interest": current_open_interest,
        "funding_rate": funding_rate,
        "funding_rate_pct": _round_optional(funding_rate_pct),
        "long_liquidations_usd": None,
        "short_liquidations_usd": None,
        "basis_pct": _round_optional(basis_pct),
        "source": f"binance_usdm_public_{exchange_symbol.lower()}",
        "source_timestamp": source_timestamp,
        "notes": _compose_live_notes(
            exchange_symbol=exchange_symbol,
            oi_change_pct=oi_change_pct,
            funding_rate_pct=funding_rate_pct,
            basis_pct=basis_pct,
            volume_change_pct_vs_20d=volume_change_pct_vs_20d,
        ),
        "flags": flags,
        "binance_usdm": {
            "exchange_symbol": exchange_symbol,
            "price_window": "rolling_24h",
            "oi_window": "approximately_24h_from_hourly_history",
            "volume_baseline": f"rolling_24h_vs_{config.volume_lookback_days}_closed_utc_days",
            "current_open_interest": current_open_interest,
            "baseline_open_interest": baseline_open_interest,
            "baseline_timestamp": (
                _millis_to_iso(baseline_timestamp_ms)
                if baseline_timestamp_ms
                else None
            ),
            "mark_price": mark_price,
            "index_price": index_price,
            "collector_version": BINANCE_USDM_COLLECTOR_VERSION,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
    }

    health = {
        "symbol": canonical_symbol,
        "exchange_symbol": exchange_symbol,
        "status": "PARTIAL" if endpoint_warnings else "OK",
        "source_timestamp": source_timestamp,
        "warnings": endpoint_warnings,
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }
    return item, health


def _get_json(
    *,
    session: Any,
    config: BinanceUsdmCollectorConfig,
    path: str,
    params: dict[str, Any],
) -> Any:
    url = f"{config.base_url.rstrip('/')}/{path.lstrip('/')}"
    last_error: Exception | None = None

    for attempt in range(1, config.max_attempts + 1):
        try:
            response = session.get(
                url,
                params=params,
                timeout=config.timeout_sec,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "ai-market-analyst-positioning/0.2",
                },
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and payload.get("code") is not None:
                code = _to_int(payload.get("code"))
                if code is not None and code < 0:
                    raise RuntimeError(
                        f"binance_api_error code={code} msg={payload.get('msg')}"
                    )
            return payload
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < config.max_attempts and config.backoff_sec > 0:
                time.sleep(config.backoff_sec * attempt)

    raise RuntimeError(
        f"request_failed path={path} params={params} error={last_error}"
    )



def _get_json_optional(
    *,
    session: Any,
    config: BinanceUsdmCollectorConfig,
    path: str,
    params: dict[str, Any],
    warnings: list[str],
) -> Any:
    try:
        return _get_json(
            session=session,
            config=config,
            path=path,
            params=params,
        )
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"optional_endpoint:{path}:{type(exc).__name__}:{exc}")
        return None

def _oi_baseline(rows: list[Any]) -> tuple[float, int | None]:
    normalized: list[tuple[int, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        timestamp = _to_int(row.get("timestamp"))
        value = _to_float(row.get("sumOpenInterest"))
        if timestamp is None or value is None or value <= 0:
            continue
        normalized.append((timestamp, value))

    if not normalized:
        raise ValueError("open interest history contains no usable rows")

    normalized.sort(key=lambda pair: pair[0])
    timestamp, value = normalized[0]
    return value, timestamp


def _volume_change_vs_closed_daily_average(
    *,
    rolling_quote_volume: float | None,
    klines: list[Any],
    lookback_days: int,
) -> float | None:
    if rolling_quote_volume is None:
        return None

    now_ms = int(time.time() * 1000)
    closed_quote_volumes: list[float] = []

    for row in klines:
        if not isinstance(row, (list, tuple)) or len(row) < 8:
            continue
        close_time = _to_int(row[6])
        quote_volume = _to_float(row[7])
        if close_time is None or quote_volume is None or quote_volume <= 0:
            continue
        if close_time < now_ms:
            closed_quote_volumes.append(quote_volume)

    closed_quote_volumes = closed_quote_volumes[-lookback_days:]
    if len(closed_quote_volumes) < min(5, lookback_days):
        return None

    average = sum(closed_quote_volumes) / len(closed_quote_volumes)
    if average <= 0:
        return None
    return _pct_change(rolling_quote_volume, average)


def _normalize_requested_symbols(symbols: Iterable[str] | None) -> list[str]:
    values = list(symbols) if symbols is not None else list(BINANCE_USDM_SYMBOLS)
    out: list[str] = []

    for raw in values:
        cleaned = str(raw or "").strip().upper().replace("-", "").replace("/", "")
        canonical = cleaned
        for key, exchange_symbol in BINANCE_USDM_SYMBOLS.items():
            if cleaned in {key, exchange_symbol}:
                canonical = key
                break
        if canonical not in BINANCE_USDM_SYMBOLS:
            raise ValueError(f"unsupported Binance USD-M positioning symbol: {raw}")
        if canonical not in out:
            out.append(canonical)

    if not out:
        raise ValueError("at least one Binance USD-M symbol is required")
    return out


def _compose_live_notes(
    *,
    exchange_symbol: str,
    oi_change_pct: float,
    funding_rate_pct: float | None,
    basis_pct: float | None,
    volume_change_pct_vs_20d: float | None,
) -> str:
    parts = [
        "live Binance USD-M public derivatives proxy",
        f"exchange_symbol={exchange_symbol}",
        f"perp_oi_change_pct={oi_change_pct:.4f}",
    ]
    if funding_rate_pct is not None:
        parts.append(f"funding_pct={funding_rate_pct:.6f}")
    if basis_pct is not None:
        parts.append(f"mark_index_premium_pct={basis_pct:.6f}")
    if volume_change_pct_vs_20d is not None:
        parts.append(f"volume_change_pct_vs_20d={volume_change_pct_vs_20d:.4f}")
    parts.append("aggregate_liquidations=unavailable_public_rest")
    return "; ".join(parts)


def _pct_change(current: float, baseline: float) -> float:
    if baseline == 0:
        raise ValueError("cannot calculate percent change from zero baseline")
    return ((current - baseline) / abs(baseline)) * 100.0


def _required_float(value: Any, field_name: str) -> float:
    parsed = _to_float(value)
    if parsed is None:
        raise ValueError(f"missing or invalid numeric field: {field_name}")
    return parsed


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _round_optional(value: float | None, digits: int = 6) -> float | None:
    return round(value, digits) if value is not None else None


def _millis_to_iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_int(
    name: str,
    default: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    raw = os.getenv(name)
    try:
        value = int(str(raw).strip()) if raw not in {None, ""} else default
    except ValueError:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_float(
    name: str,
    default: float,
    *,
    minimum: float | None = None,
) -> float:
    raw = os.getenv(name)
    try:
        value = float(str(raw).strip()) if raw not in {None, ""} else default
    except ValueError:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect live Binance USD-M positioning proxy snapshot."
    )
    parser.add_argument("--runtime-dir", default=None, help="Runtime directory.")
    parser.add_argument("--output", default=None, help="Snapshot output path.")
    parser.add_argument("--date", default=None, help="Snapshot date YYYY-MM-DD.")
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="Canonical or Binance symbol. Repeat for multiple symbols.",
    )
    parser.add_argument(
        "--persist-empty",
        action="store_true",
        help="Overwrite snapshot even when collection returns zero items.",
    )
    args = parser.parse_args()

    path, payload = collect_and_write_binance_usdm_snapshot(
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
