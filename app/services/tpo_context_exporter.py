from __future__ import annotations

import argparse
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

EXPORTER_VERSION = "tpo-context-exporter-v1"
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
            },
            "filters": {
                "auction_context_available": False,
                "open_relation": "UNKNOWN",
                "auction_bias": "UNKNOWN",
                "telegram_modifier": "NEUTRAL",
                "confidence_modifier": 0.0,
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

    filters_payload = to_jsonable(filters)
    if isinstance(filters_payload, dict):
        filters_payload["tpo_source"] = "offline_exporter"

    return {
        "symbol": symbol.value,
        "updated_at_utc": now_iso(),
        "context": context_payload,
        "filters": filters_payload,
    }


def export_tpo_context(groups: list[str], max_bars: int, output_path: Path) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "exporter_version": EXPORTER_VERSION,
        "updated_at_utc": now_iso(),
        "max_bars": max_bars,
        "symbols": {},
        "errors": [],
    }

    seen: set[str] = set()

    for group in groups:
        group = str(group).strip().lower()
        if not group:
            continue

        loader = _build_loader_for_batch_group(group)
        raw_symbols = get_batch_symbols(group)

        for raw in raw_symbols:
            try:
                symbol = normalize_symbol(raw)
                if symbol.value in seen:
                    continue
                seen.add(symbol.value)

                item = build_symbol_tpo(loader, symbol, max_bars=max_bars)
                payload["symbols"][symbol.value] = item
                print(
                    symbol.value,
                    "open=", item.get("context", {}).get("open_relation"),
                    "bias=", item.get("context", {}).get("auction_bias"),
                    "modifier=", item.get("filters", {}).get("telegram_modifier"),
                )

            except Exception as exc:  # noqa: BLE001
                error_payload = {
                    "group": group,
                    "symbol": str(getattr(raw, "value", raw)),
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
                payload["errors"].append(error_payload)
                print("ERROR", error_payload)
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
        "output": str(args.output),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()