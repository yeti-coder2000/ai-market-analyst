from __future__ import annotations

import os
from typing import Any

import requests

from schema import FetchRequest, TWELVEDATA_INTERVAL_MAP, TWELVEDATA_SYMBOL_MAP


class TwelveDataClient:
    BASE_URL = "https://api.twelvedata.com/time_series"

    def __init__(self, api_key: str | None = None, timeout: int = 20) -> None:
        self.api_key = api_key or os.getenv("TWELVEDATA_API_KEY")
        self.timeout = timeout

        if not self.api_key:
            raise ValueError(
                "TwelveData API key not found. Set TWELVEDATA_API_KEY in environment."
            )

    def fetch_time_series(self, req: FetchRequest) -> dict[str, Any]:
        params = {
            "symbol": TWELVEDATA_SYMBOL_MAP[req.instrument],
            "interval": TWELVEDATA_INTERVAL_MAP[req.timeframe],
            "outputsize": req.outputsize,
            "timezone": "UTC",
            "apikey": self.api_key,
        }

        response = requests.get(self.BASE_URL, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()

        if "status" in payload and payload["status"] == "error":
            message = payload.get("message", "Unknown TwelveData API error")
            raise RuntimeError(f"TwelveData API error: {message}")

        if "values" not in payload:
            raise RuntimeError(f"Unexpected TwelveData response: keys={list(payload.keys())}")

        return payload