from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from app.core.settings import EXPECTED_PRICE_RANGES, LoaderConfig
from app.core.contracts import FetchRequest, LoadResult
from app.core.enums import Instrument, Timeframe
from app.providers.twelvedata_client import TwelveDataClient
from app.storage.cache_store import ParquetCache


STALE_THRESHOLD = {
    Timeframe.M15: timedelta(minutes=15),
    Timeframe.M30: timedelta(minutes=30),
    Timeframe.H1: timedelta(hours=1),
    Timeframe.H4: timedelta(hours=4),
    Timeframe.D1: timedelta(days=1),
}


@dataclass(slots=True)
class LoadResult:
    instrument: Instrument
    timeframe: Timeframe
    df: pd.DataFrame
    source: str  # "cache" | "api"
    rows: int
    last_ts: pd.Timestamp | None
    last_close: float | None


class MarketDataLoader:
    def __init__(
        self,
        client: TwelveDataClient,
        cache: ParquetCache,
        config: LoaderConfig | None = None,
    ) -> None:
        self.client = client
        self.cache = cache
        self.config = config or LoaderConfig()

    def load_cached(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
    ) -> pd.DataFrame:
        return self.cache.load(instrument, timeframe)

    def refresh_timeframe(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        outputsize: int | None = None,
        force_refresh: bool = False,
    ) -> LoadResult:
        cached_df = self.cache.load(instrument, timeframe)

        if not force_refresh and not cached_df.empty and not self._is_stale(cached_df, timeframe):
            last_ts, last_close = self._extract_last_info(cached_df)
            print(f"[CACHE] {instrument.value} {timeframe.value} (fresh, rows={len(cached_df)})")
            return LoadResult(
                instrument=instrument,
                timeframe=timeframe,
                df=cached_df,
                source="cache",
                rows=len(cached_df),
                last_ts=last_ts,
                last_close=last_close,
            )

        print(f"[API] refreshing {instrument.value} {timeframe.value}")

        fresh_df = self._fetch_dataframe(
            instrument=instrument,
            timeframe=timeframe,
            outputsize=outputsize or self.config.default_outputsize,
        )

        merged = self.cache.merge_and_save(instrument, timeframe, fresh_df)
        last_ts, last_close = self._extract_last_info(merged)

        return LoadResult(
            instrument=instrument,
            timeframe=timeframe,
            df=merged,
            source="api",
            rows=len(merged),
            last_ts=last_ts,
            last_close=last_close,
        )

    def load_with_sanity(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        outputsize: int | None = None,
    ) -> LoadResult:
        result = self.refresh_timeframe(
            instrument=instrument,
            timeframe=timeframe,
            outputsize=outputsize,
            force_refresh=False,
        )

        ok, reason = self.validate_price_sanity(instrument, result.last_close)
        if ok:
            return result

        print(
            f"[SANITY] failed for {instrument.value} {timeframe.value}: {reason}"
        )

        if not self.config.sanity_retry_without_cache:
            raise RuntimeError(reason)

        print(f"[SANITY] retrying without cache for {instrument.value} {timeframe.value}")

        result = self.refresh_timeframe(
            instrument=instrument,
            timeframe=timeframe,
            outputsize=outputsize,
            force_refresh=True,
        )

        ok, reason = self.validate_price_sanity(instrument, result.last_close)
        if not ok:
            raise RuntimeError(reason)

        return result

    def _fetch_dataframe(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        outputsize: int,
    ) -> pd.DataFrame:
        req = FetchRequest(
            instrument=instrument,
            timeframe=timeframe,
            outputsize=outputsize,
        )

        payload = self.client.fetch_time_series(req)
        self._validate_payload_shape(payload, instrument, timeframe)

        df = self._payload_to_dataframe(payload)
        df = self._normalize_dataframe(df, instrument, timeframe)

        return df

    def _validate_payload_shape(
        self,
        payload: dict[str, Any],
        instrument: Instrument,
        timeframe: Timeframe,
    ) -> None:
        if not isinstance(payload, dict):
            raise ValueError(f"Payload is not dict for {instrument.value} {timeframe.value}")

        meta = payload.get("meta")
        values = payload.get("values")

        if not isinstance(meta, dict):
            raise ValueError(f"Payload meta is not dict for {instrument.value} {timeframe.value}")

        if not isinstance(values, list) or not values:
            raise ValueError(f"Payload values missing/empty for {instrument.value} {timeframe.value}")

    def _payload_to_dataframe(self, payload: dict[str, Any]) -> pd.DataFrame:
        values = payload["values"]
        df = pd.DataFrame(values)

        if "datetime" not in df.columns:
            raise ValueError("Payload does not contain 'datetime' column")

        df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        df = df.dropna(subset=["datetime"])
        df = df.set_index("datetime")

        for col in ["open", "high", "low", "close"]:
            if col not in df.columns:
                raise ValueError(f"Payload missing required column: {col}")
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if "volume" not in df.columns:
            df["volume"] = 0.0
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)

        return df

    def _normalize_dataframe(
        self,
        df: pd.DataFrame,
        instrument: Instrument,
        timeframe: Timeframe,
    ) -> pd.DataFrame:
        out = df.copy()

        out = out.dropna(subset=["open", "high", "low", "close"])
        out = out.sort_index()
        out = out[~out.index.duplicated(keep="last")]

        if out.empty:
            raise ValueError(f"Normalized dataframe is empty for {instrument.value} {timeframe.value}")

        last_ts, last_close = self._extract_last_info(out)
        print(
            f"[DATA] {instrument.value} {timeframe.value} "
            f"last_ts={last_ts} last_close={last_close} rows={len(out)}"
        )

        return out[["open", "high", "low", "close", "volume"]]

    def _is_stale(self, df: pd.DataFrame, timeframe: Timeframe) -> bool:
        if df is None or df.empty:
            return True

        last_ts = df.index[-1]
        if not isinstance(last_ts, pd.Timestamp):
            last_ts = pd.Timestamp(last_ts)

        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize("UTC")
        else:
            last_ts = last_ts.tz_convert("UTC")

        now = datetime.now(timezone.utc)
        threshold = STALE_THRESHOLD[timeframe]

        return (now - last_ts) >= threshold

    def _extract_last_info(self, df: pd.DataFrame) -> tuple[pd.Timestamp | None, float | None]:
        if df is None or df.empty:
            return None, None

        last_row = df.iloc[-1]
        last_ts = df.index[-1]

        try:
            last_close = float(last_row["close"])
        except (TypeError, ValueError):
            last_close = None

        return last_ts, last_close

    @staticmethod
    def validate_price_sanity(
        instrument: Instrument,
        price: float | None,
    ) -> tuple[bool, str | None]:
        if price is None:
            return False, f"No last close found for {instrument.value}"

        bounds = EXPECTED_PRICE_RANGES.get(instrument.value)
        if bounds is None:
            return True, None

        low, high = bounds
        if low <= price <= high:
            return True, None

        return (
            False,
            f"Price sanity check failed for {instrument.value}: "
            f"{price:.5f} not in range [{low}, {high}]",
        )