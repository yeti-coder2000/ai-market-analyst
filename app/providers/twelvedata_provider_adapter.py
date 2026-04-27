from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.core.contracts import FetchRequest
from app.core.enums import Instrument, Timeframe


class TwelveDataAdapterError(RuntimeError):
    """Base adapter error."""


class TwelveDataResponseError(TwelveDataAdapterError):
    """Raised when provider response is invalid."""


@dataclass(frozen=True, slots=True)
class AdapterConfig:
    debug: bool = True


class TwelveDataProviderAdapter:
    """
    Adapter between internal enums/contracts and TwelveData client.

    Responsible for:
    - internal symbol/timeframe mapping
    - response validation
    - payload normalization

    Important:
    - TwelveData is currently trusted for core FX / metals / crypto.
    - GER40 / NAS100 / SPX500 / UKOIL are intentionally NOT mapped here,
      because discovery showed TwelveData returns ETFs/ETPs/wrong tickers
      instead of the actual CFD/index/oil instruments.
    """

    SYMBOL_MAP: dict[Instrument, str] = {
        # metals
        Instrument.XAUUSD: "XAU/USD",

        # core FX
        Instrument.EURUSD: "EUR/USD",
        Instrument.GBPUSD: "GBP/USD",

        # fx_major expansion
        Instrument.USDJPY: "USD/JPY",
        Instrument.USDCHF: "USD/CHF",
        Instrument.USDCAD: "USD/CAD",
        Instrument.AUDUSD: "AUD/USD",

        # optional future fx reserve
        Instrument.NZDUSD: "NZD/USD",
        Instrument.EURJPY: "EUR/JPY",
        Instrument.GBPJPY: "GBP/JPY",
        Instrument.AUDJPY: "AUD/JPY",

        # crypto
        Instrument.BTCUSD: "BTC/USD",
        Instrument.ETHUSD: "ETH/USD",
    }

    TIMEFRAME_MAP: dict[Timeframe, str] = {
        Timeframe.M15: "15min",
        Timeframe.M30: "30min",
        Timeframe.H1: "1h",
        Timeframe.H4: "4h",
        Timeframe.D1: "1day",
    }

    def __init__(self, client: Any, config: AdapterConfig | None = None) -> None:
        self.client = client
        self.config = config or AdapterConfig()

    def fetch_time_series(self, request: FetchRequest) -> dict[str, Any]:
        provider_symbol = self._map_symbol(request.instrument)
        provider_interval = self._map_timeframe(request.timeframe)

        self._debug(
            f"[DEBUG] TD request "
            f"internal_symbol={request.instrument.value} provider_symbol={provider_symbol} "
            f"internal_tf={request.timeframe.value} provider_tf={provider_interval}"
        )

        raw_payload = self._call_client(
            provider_symbol=provider_symbol,
            provider_interval=provider_interval,
            outputsize=request.outputsize,
        )

        self._validate_raw_response(
            payload=raw_payload,
            provider_symbol=provider_symbol,
            provider_interval=provider_interval,
        )

        normalized = self._normalize_payload(
            raw_payload=raw_payload,
            request=request,
            provider_symbol=provider_symbol,
            provider_interval=provider_interval,
        )

        self._print_response_debug(normalized)
        return normalized

    def _map_symbol(self, instrument: Instrument) -> str:
        try:
            return self.SYMBOL_MAP[instrument]
        except KeyError as error:
            raise TwelveDataAdapterError(
                f"No TwelveData symbol mapping for {instrument.value}. "
                "This instrument may require a different provider."
            ) from error

    def _map_timeframe(self, timeframe: Timeframe) -> str:
        try:
            return self.TIMEFRAME_MAP[timeframe]
        except KeyError as error:
            raise TwelveDataAdapterError(
                f"No TwelveData timeframe mapping for {timeframe.value}"
            ) from error

    def _call_client(
        self,
        *,
        provider_symbol: str,
        provider_interval: str,
        outputsize: int,
    ) -> dict[str, Any]:
        try:
            payload = self.client.fetch_time_series(
                symbol=provider_symbol,
                interval=provider_interval,
                outputsize=outputsize,
            )
        except Exception as error:  # noqa: BLE001
            raise TwelveDataAdapterError(
                f"TwelveData client call failed for symbol={provider_symbol}, "
                f"interval={provider_interval}: {error}"
            ) from error

        if not isinstance(payload, dict):
            raise TwelveDataResponseError(
                f"Non-dict payload returned for symbol={provider_symbol}, "
                f"interval={provider_interval}"
            )

        return payload

    def _validate_raw_response(
        self,
        *,
        payload: dict[str, Any],
        provider_symbol: str,
        provider_interval: str,
    ) -> None:
        if str(payload.get("status", "")).lower() == "error":
            raise TwelveDataResponseError(
                f"TwelveData API error for symbol={provider_symbol}, "
                f"interval={provider_interval}, code={payload.get('code')}, "
                f"message={payload.get('message')}"
            )

        values = payload.get("values")
        if not isinstance(values, list) or not values:
            raise TwelveDataResponseError(
                f"TwelveData payload missing values for symbol={provider_symbol}, "
                f"interval={provider_interval}"
            )

        meta = payload.get("meta")
        if meta is not None and not isinstance(meta, dict):
            raise TwelveDataResponseError(
                f"TwelveData payload meta is not a dict for symbol={provider_symbol}, "
                f"interval={provider_interval}"
            )

    def _normalize_payload(
        self,
        *,
        raw_payload: dict[str, Any],
        request: FetchRequest,
        provider_symbol: str,
        provider_interval: str,
    ) -> dict[str, Any]:
        raw_values = raw_payload["values"]
        raw_meta = raw_payload.get("meta") or {}

        normalized_values = [self._normalize_bar(item) for item in raw_values]

        normalized_meta = {
            "requested_internal_symbol": request.instrument.value,
            "requested_internal_timeframe": request.timeframe.value,
            "requested_outputsize": request.outputsize,
            "provider_symbol": raw_meta.get("symbol", provider_symbol),
            "provider_interval": raw_meta.get("interval", provider_interval),
            "currency_base": raw_meta.get("currency_base"),
            "currency_quote": raw_meta.get("currency_quote"),
            "exchange": raw_meta.get("exchange"),
            "type": raw_meta.get("type"),
            "timezone": raw_meta.get("timezone"),
            "fetched_at_utc": datetime.now(UTC).isoformat(),
        }

        return {
            "meta": normalized_meta,
            "values": normalized_values,
        }

    def _normalize_bar(self, item: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(item, dict):
            raise TwelveDataResponseError("Encountered non-dict OHLC bar")

        return {
            "datetime": item.get("datetime") or item.get("timestamp"),
            "open": self._to_float(item.get("open")),
            "high": self._to_float(item.get("high")),
            "low": self._to_float(item.get("low")),
            "close": self._to_float(item.get("close")),
            "volume": self._to_float(item.get("volume"), default=0.0),
        }

    @staticmethod
    def _to_float(value: Any, default: float | None = None) -> float | None:
        if value is None:
            return default

        if isinstance(value, (int, float)):
            return float(value)

        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _print_response_debug(self, payload: dict[str, Any]) -> None:
        meta = payload.get("meta", {})
        values = payload.get("values", [])

        self._debug(f"[DEBUG] TD meta={meta}")

        if not values:
            self._debug("[DEBUG] TD response has no values")
            return

        latest_bar = values[0]
        latest_dt = latest_bar.get("datetime")
        latest_close = latest_bar.get("close")

        self._debug(
            f"[DEBUG] TD response provider_symbol={meta.get('provider_symbol')} "
            f"provider_interval={meta.get('provider_interval')} "
            f"datetime={latest_dt} close={latest_close} rows={len(values)}"
        )

    def _debug(self, message: str) -> None:
        if self.config.debug:
            print(message)