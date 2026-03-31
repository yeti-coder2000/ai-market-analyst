from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

import requests


class TwelveDataClientError(RuntimeError):
    """Base TwelveData client error."""


class TwelveDataHTTPError(TwelveDataClientError):
    """Raised when HTTP request fails or response is invalid."""


class TwelveDataAPIError(TwelveDataClientError):
    """Raised when TwelveData returns API-level error payload."""


@dataclass(slots=True)
class TwelveDataClientConfig:
    api_key: str | None = None
    base_url: str = "https://api.twelvedata.com"
    timeout_seconds: int = 20
    outputsize: int = 500
    timezone: str = "UTC"
    order: str = "desc"
    debug: bool = True


class TwelveDataClient:
    """
    Production-style TwelveData client.

    Primary method:
        fetch_time_series(symbol=..., interval=..., outputsize=...)

    Backward-compatible method:
        refresh(symbol, interval)
    """

    def __init__(self, config: TwelveDataClientConfig | None = None) -> None:
        self.config = config or TwelveDataClientConfig()
        self.api_key = self.config.api_key or os.getenv("TWELVEDATA_API_KEY")

        if not self.api_key:
            raise TwelveDataClientError(
                "TwelveData API key is missing. "
                "Set TWELVEDATA_API_KEY environment variable or pass api_key explicitly."
            )

        self.session = requests.Session()

    def fetch_time_series(
        self,
        *,
        symbol: str,
        interval: str,
        outputsize: int | None = None,
    ) -> dict[str, Any]:
        """
        Fetch raw time series payload from TwelveData.

        Args:
            symbol: Provider symbol, e.g. 'XAU/USD'
            interval: Provider interval, e.g. '15min'
            outputsize: Optional override for response size

        Returns:
            Raw JSON payload as dict
        """
        endpoint = f"{self.config.base_url}/time_series"

        params = {
            "symbol": symbol,
            "interval": interval,
            "apikey": self.api_key,
            "outputsize": outputsize or self.config.outputsize,
            "timezone": self.config.timezone,
            "order": self.config.order,
            "format": "JSON",
        }

        if self.config.debug:
            print(
                f"[DEBUG] TwelveDataClient.fetch_time_series request "
                f"symbol={symbol} interval={interval} endpoint={endpoint}"
            )

        try:
            response = self.session.get(
                endpoint,
                params=params,
                timeout=self.config.timeout_seconds,
            )
        except requests.RequestException as error:
            raise TwelveDataHTTPError(
                f"HTTP request to TwelveData failed for symbol={symbol}, interval={interval}: {error}"
            ) from error

        if response.status_code != 200:
            raise TwelveDataHTTPError(
                f"TwelveData returned HTTP {response.status_code} "
                f"for symbol={symbol}, interval={interval}: {response.text}"
            )

        try:
            payload: dict[str, Any] = response.json()
        except ValueError as error:
            raise TwelveDataHTTPError(
                f"TwelveData returned non-JSON response for symbol={symbol}, interval={interval}"
            ) from error

        self._raise_if_api_error(payload, symbol=symbol, interval=interval)

        if self.config.debug:
            meta = payload.get("meta", {})
            print(f"[DEBUG] TwelveDataClient.fetch_time_series meta={meta}")

        return payload

    def refresh(self, symbol: str, interval: str) -> dict[str, Any]:
        """
        Backward-compatible wrapper for older code paths.
        """
        return self.fetch_time_series(
            symbol=symbol,
            interval=interval,
            outputsize=self.config.outputsize,
        )

    @staticmethod
    def _raise_if_api_error(
        payload: dict[str, Any],
        *,
        symbol: str,
        interval: str,
    ) -> None:
        status = str(payload.get("status", "")).lower()

        if status == "error":
            raise TwelveDataAPIError(
                f"TwelveData API error for symbol={symbol}, interval={interval}, "
                f"code={payload.get('code')}, message={payload.get('message')}"
            )

        if "values" not in payload and "message" in payload:
            raise TwelveDataAPIError(
                f"TwelveData malformed error-like payload for symbol={symbol}, interval={interval}, "
                f"code={payload.get('code')}, message={payload.get('message')}"
            )