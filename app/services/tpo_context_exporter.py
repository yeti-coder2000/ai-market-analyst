from __future__ import annotations

import argparse
import copy
import gc
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.auction.profile_engine import build_auction_context, auction_context_to_signal_filters
from app.core.enums import Instrument, Timeframe
from app.core.instrument_batches import get_batch_symbols
from app.core.settings import settings
from app.runners.stateful_batch_runner import _build_loader_for_batch_group, to_jsonable

EXPORTER_VERSION = "tpo-context-exporter-v1.1-preserve-previous-context"
DEFAULT_MAX_BARS = 672

TPO_DIR = settings.runtime_dir / "tpo"
TPO_LATEST_PATH = TPO_DIR / "tpo_latest.json"

TICK_SIZE_BY_SYMBOL: dict[str, float] = {
    "XAUUSD": 5.0,
    "BTCUSD": 100.0,
    "ETHUSD": 10.0,
    "EURUSD": 0.001,
    "GBPUSD": 0.001,
    "USDJPY": 0.10,
    "USDCHF": 0.001,
    "USDCAD": 0.001,
    "AUDUSD": 0.001,
    "UKOIL": 0.10,
    "GER40": 10.0,
    "NAS100": 25.0,
    "SPX500": 5.0,
}


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_symbol(raw: Any) -> Instrument:
    if isinstance(raw, Instrument):
        return raw
    value = getattr(raw, "value", raw)
    return Instrument(str(value))


def ensure_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, "", {}, ()):
        return []
    return [value]


def load_previous_store(path: Path) -> dict[str, Any]:
    """
    Best-effort read of the previous TPO context store.

    The exporter is allowed to run when one provider has a temporary failure
    (for example yfinance rate limiting ^GDAXI). In that case we preserve the
    last valid context for the failed symbol and mark it as degraded/stale.
    """
    if not path.exists():
        return {}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    return data if isinstance(data, dict) else {}


def get_previous_symbol_item(previous_store: dict[str, Any], symbol: Instrument) -> dict[str, Any] | None:
    symbols = previous_store.get("symbols")
    if not isinstance(symbols, dict):
        return None

    item = symbols.get(symbol.value)
    return copy.deepcopy(item) if isinstance(item, dict) else None


def mark_context_as_preserved_fallback(
    *,
    previous_item: dict[str, Any],
    symbol: Instrument,
    group: str,
    error_payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Preserve the previous context but make it safe.

    Important:
    - We keep open_relation / auction_bias for visibility and daily reports.
    - We force stale/provider-error markers and Telegram downgrade.
    - We force tpo_signal_permission to STALE_DATA so live gate should not
      allow battle logic from stale provider data.
    """
    item = copy.deepcopy(previous_item)
    previous_item_updated_at = item.get("updated_at_utc")
    updated_at = now_iso()

    context = item.get("context")
    if not isinstance(context, dict):
        context = {}
        item["context"] = context

    filters = item.get("filters")
    if not isinstance(filters, dict):
        filters = {}
        item["filters"] = filters

    context["auction_context_available"] = bool(context.get("auction_context_available", True))
    context["tpo_source"] = "offline_exporter_previous_context_fallback"
    context["fallback_preserved_previous_context"] = True
    context["provider_error"] = True
    context["provider_error_group"] = group
    context["provider_error_type"] = error_payload.get("error_type")
    context["provider_error_message"] = error_payload.get("error")
    context["provider_error_at_utc"] = updated_at
    context["previous_context_updated_at_utc"] = previous_item_updated_at
    context["market_data_is_stale"] = True
    context["context_stale"] = True
    context["market_status"] = "STALE_DATA"
    context["symbol"] = symbol.value

    existing_reasons = ensure_list(filters.get("reasons"))
    fallback_reason = (
        f"Provider error for {symbol.value}; preserved previous TPO context "
        "as stale/degraded and blocked battle permission."
    )

    filters["auction_context_available"] = bool(filters.get("auction_context_available", True))
    filters["tpo_source"] = "offline_exporter_previous_context_fallback"
    filters["fallback_preserved_previous_context"] = True
    filters["provider_error"] = True
    filters["provider_error_type"] = error_payload.get("error_type")
    filters["provider_error_message"] = error_payload.get("error")
    filters["market_data_is_stale"] = True
    filters["context_stale"] = True
    filters["tpo_signal_permission"] = "STALE_DATA"
    filters["telegram_modifier"] = "DOWNGRADE"
    filters["confidence_modifier"] = min(float(filters.get("confidence_modifier") or 0.0), -1.0)
    filters["reasons"] = [*existing_reasons, fallback_reason]

    item["symbol"] = symbol.value
    item["updated_at_utc"] = updated_at
    item["fallback_preserved_previous_context"] = True
    item["previous_item_updated_at_utc"] = previous_item_updated_at
    item["provider_error"] = True
    item["provider_error_group"] = group
    item["provider_error_type"] = error_payload.get("error_type")
    item["provider_error_message"] = error_payload.get("error")

    return item


def build_provider_error_item(
    *,
    symbol: Instrument,
    group: str,
    error_payload: dict[str, Any],
    reason: str | None = None,
) -> dict[str, Any]:
    updated_at = now_iso()
    message = reason or str(error_payload.get("error") or "provider_error")

    return {
        "symbol": symbol.value,
        "updated_at_utc": updated_at,
        "provider_error": True,
        "provider_error_group": group,
        "provider_error_type": error_payload.get("error_type"),
        "provider_error_message": message,
        "context": {
            "symbol": symbol.value,
            "auction_context_available": False,
            "reason": message,
            "tpo_source": "offline_exporter_provider_error",
            "provider_error": True,
            "provider_error_type": error_payload.get("error_type"),
            "provider_error_message": message,
            "provider_error_at_utc": updated_at,
            "market_data_is_stale": True,
            "context_stale": True,
            "market_status": "PROVIDER_ERROR",
            "open_relation": "UNKNOWN",
            "auction_bias": "UNKNOWN",
        },
        "filters": {
            "auction_context_available": False,
            "open_relation": "UNKNOWN",
            "auction_bias": "UNKNOWN",
            "tpo_signal_permission": "PROVIDER_ERROR",
            "telegram_modifier": "DOWNGRADE",
            "confidence_modifier": -1.0,
            "provider_error": True,
            "provider_error_type": error_payload.get("error_type"),
            "provider_error_message": message,
            "market_data_is_stale": True,
            "context_stale": True,
            "tpo_source": "offline_exporter_provider_error",
            "reasons": [
                "TPO exporter could not build current context and no previous context was available.",
                message,
            ],
        },
    }


def prepare_ohlc(df: pd.DataFrame | None, max_bars: int) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None

    out = df.copy()
    out = out.reset_index()

    rename_map: dict[Any, str] = {}
    for col in out.columns:
        key = str(col).strip().lower()
        if key in {"datetime", "date", "time", "timestamp", "index"}:
            rename_map[col] = "timestamp"
        elif key in {"open", "o"}:
            rename_map[col] = "open"
        elif key in {"high", "h"}:
            rename_map[col] = "high"
        elif key in {"low", "l"}:
            rename_map[col] = "low"
        elif key in {"close", "c"}:
            rename_map[col] = "close"
        elif key in {"volume", "vol", "v"}:
            rename_map[col] = "volume"

    out = out.rename(columns=rename_map)
    required = {"timestamp", "open", "high", "low", "close"}
    if not required.issubset(set(out.columns)):
        return None

    cols = ["timestamp", "open", "high", "low", "close"]
    if "volume" in out.columns:
        cols.append("volume")

    out = out[cols].copy()
    if max_bars > 0 and len(out) > max_bars:
        out = out.tail(max_bars).copy()

    if "volume" not in out.columns:
        out["volume"] = 1.0

    return out


def build_symbol_tpo(loader: Any, symbol: Instrument, max_bars: int) -> dict[str, Any]:
    tick_size = TICK_SIZE_BY_SYMBOL.get(symbol.value, 0.001)
    result = loader.load_with_sanity(instrument=symbol, timeframe=Timeframe.M15)
    df = prepare_ohlc(getattr(result, "df", None), max_bars=max_bars)

    if df is None or df.empty:
        return {
            "symbol": symbol.value,
            "updated_at_utc": now_iso(),
            "context": {
                "symbol": symbol.value,
                "auction_context_available": False,
                "reason": "missing_or_unusable_15m_ohlc_dataframe",
                "tpo_source": "offline_exporter",
                "market_data_is_stale": True,
                "context_stale": True,
                "market_status": "NO_DATA",
                "open_relation": "UNKNOWN",
                "auction_bias": "UNKNOWN",
            },
            "filters": {
                "auction_context_available": False,
                "open_relation": "UNKNOWN",
                "auction_bias": "UNKNOWN",
                "tpo_signal_permission": "NO_DATA",
                "telegram_modifier": "DOWNGRADE",
                "confidence_modifier": -1.0,
                "market_data_is_stale": True,
                "context_stale": True,
                "reasons": ["TPO exporter could not build OHLC dataframe."],
            },
        }

    context = build_auction_context(
        df,
        symbol=symbol.value,
        timeframe="15m",
        tick_size=tick_size,
        value_area_pct=0.70,
        ib_minutes=60,
    )
    filters = auction_context_to_signal_filters(context)

    context_payload = to_jsonable(context.to_dict())
    if isinstance(context_payload, dict):
        context_payload["auction_context_available"] = True
        context_payload["tpo_source"] = "offline_exporter"
        context_payload["bars_used"] = int(len(df))
        context_payload["max_bars"] = int(max_bars)
        context_payload["tick_size"] = float(tick_size)
        context_payload["memory_mode"] = "offline_bounded_recent_history"
        context_payload["provider_error"] = False
        context_payload["fallback_preserved_previous_context"] = False

    filters_payload = to_jsonable(filters)
    if isinstance(filters_payload, dict):
        filters_payload["tpo_source"] = "offline_exporter"
        filters_payload["provider_error"] = False
        filters_payload["fallback_preserved_previous_context"] = False

    return {
        "symbol": symbol.value,
        "updated_at_utc": now_iso(),
        "context": context_payload,
        "filters": filters_payload,
    }


def maybe_preserve_previous_for_unavailable_item(
    *,
    item: dict[str, Any],
    previous_store: dict[str, Any],
    symbol: Instrument,
    group: str,
) -> dict[str, Any]:
    context = item.get("context") if isinstance(item, dict) else {}
    if not isinstance(context, dict):
        return item

    if context.get("auction_context_available") is not False:
        return item

    previous_item = get_previous_symbol_item(previous_store, symbol)
    if previous_item is None:
        return item

    error_payload = {
        "group": group,
        "symbol": symbol.value,
        "error": context.get("reason") or "auction_context_unavailable",
        "error_type": "AuctionContextUnavailable",
    }

    return mark_context_as_preserved_fallback(
        previous_item=previous_item,
        symbol=symbol,
        group=group,
        error_payload=error_payload,
    )


def export_tpo_context(groups: list[str], max_bars: int, output_path: Path) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    previous_store = load_previous_store(output_path)

    payload: dict[str, Any] = {
        "schema_version": "1.1",
        "exporter_version": EXPORTER_VERSION,
        "updated_at_utc": now_iso(),
        "max_bars": max_bars,
        "previous_store_loaded": bool(previous_store),
        "previous_store_updated_at_utc": previous_store.get("updated_at_utc") if previous_store else None,
        "symbols": {},
        "errors": [],
        "fallbacks": [],
    }

    seen: set[str] = set()

    for group in groups:
        group = str(group).strip().lower()
        if not group:
            continue

        loader = _build_loader_for_batch_group(group)
        raw_symbols = get_batch_symbols(group)

        for raw in raw_symbols:
            symbol_value = str(getattr(raw, "value", raw))

            try:
                symbol = normalize_symbol(raw)
                symbol_value = symbol.value

                if symbol.value in seen:
                    continue
                seen.add(symbol.value)

                item = build_symbol_tpo(loader, symbol, max_bars=max_bars)
                item = maybe_preserve_previous_for_unavailable_item(
                    item=item,
                    previous_store=previous_store,
                    symbol=symbol,
                    group=group,
                )

                payload["symbols"][symbol.value] = item

                if item.get("fallback_preserved_previous_context"):
                    fallback_payload = {
                        "group": group,
                        "symbol": symbol.value,
                        "fallback_type": "previous_context_preserved",
                        "reason": item.get("provider_error_message"),
                        "previous_item_updated_at_utc": item.get("previous_item_updated_at_utc"),
                    }
                    payload["fallbacks"].append(fallback_payload)

                print(
                    symbol.value,
                    "open=", item.get("context", {}).get("open_relation"),
                    "bias=", item.get("context", {}).get("auction_bias"),
                    "permission=", item.get("filters", {}).get("tpo_signal_permission"),
                    "modifier=", item.get("filters", {}).get("telegram_modifier"),
                    "fallback=", bool(item.get("fallback_preserved_previous_context")),
                )

            except Exception as exc:  # noqa: BLE001
                error_payload = {
                    "group": group,
                    "symbol": symbol_value,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
                payload["errors"].append(error_payload)

                try:
                    symbol = normalize_symbol(raw)
                    previous_item = get_previous_symbol_item(previous_store, symbol)

                    if previous_item is not None:
                        fallback_item = mark_context_as_preserved_fallback(
                            previous_item=previous_item,
                            symbol=symbol,
                            group=group,
                            error_payload=error_payload,
                        )
                        payload["symbols"][symbol.value] = fallback_item
                        fallback_payload = {
                            "group": group,
                            "symbol": symbol.value,
                            "fallback_type": "previous_context_preserved",
                            "reason": str(exc),
                            "previous_item_updated_at_utc": fallback_item.get("previous_item_updated_at_utc"),
                        }
                        payload["fallbacks"].append(fallback_payload)

                        print(
                            symbol.value,
                            "open=", fallback_item.get("context", {}).get("open_relation"),
                            "bias=", fallback_item.get("context", {}).get("auction_bias"),
                            "permission=", fallback_item.get("filters", {}).get("tpo_signal_permission"),
                            "modifier=", fallback_item.get("filters", {}).get("telegram_modifier"),
                            "fallback= True",
                        )
                    else:
                        payload["symbols"][symbol.value] = build_provider_error_item(
                            symbol=symbol,
                            group=group,
                            error_payload=error_payload,
                        )
                        print("ERROR", error_payload, "fallback= False no_previous_context")

                except Exception as fallback_exc:  # noqa: BLE001
                    print("ERROR", error_payload, "fallback_error=", str(fallback_exc))

            finally:
                gc.collect()

    tmp = output_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(to_jsonable(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(output_path)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export precomputed TPO context for live signal worker.")
    parser.add_argument("--groups", default="core,fx_major,indices")
    parser.add_argument("--max-bars", type=int, default=DEFAULT_MAX_BARS)
    parser.add_argument("--output", default=str(TPO_LATEST_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    groups = [x.strip() for x in str(args.groups).split(",") if x.strip()]
    payload = export_tpo_context(groups=groups, max_bars=args.max_bars, output_path=Path(args.output))
    print(json.dumps({
        "exporter_version": EXPORTER_VERSION,
        "updated_at_utc": payload.get("updated_at_utc"),
        "symbols": len(payload.get("symbols", {})),
        "errors": len(payload.get("errors", [])),
        "fallbacks": len(payload.get("fallbacks", [])),
        "output": str(args.output),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()