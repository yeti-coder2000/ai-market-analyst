from __future__ import annotations

"""
YFinance provider adapter for AI Market Analyst.

Purpose:
- provide correct market data for indices / oil instruments that TwelveData does not
  reliably expose in the required CFD/index namespace;
- return the same normalized payload shape as TwelveDataProviderAdapter so the rest
  of the pipeline stays provider-agnostic.

Internal mapping:
- GER40  -> ^GDAXI
- NAS100 -> ^NDX
- SPX500 -> ^GSPC
- UKOIL  -> BZ=F
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from app.core.contracts import FetchRequest
from app.core.enums import Instrument, Timeframe


class YFinanceAdapterError(RuntimeError):
    """Base adapter error."""


class YFinanceResponseError(YFinanceAdapterError):
    """Raised when yfinance returns no usable OHLC data."""


YFINANCE_PROVIDER_ADAPTER_VERSION = "yfinance-provider-adapter-v1.1-intraday-period-symbol-fallback"


@dataclass(frozen=True, slots=True)
class YFinanceAdapterConfig:
    debug: bool = True
    timeout_seconds: int = 30
    auto_adjust: bool = False


class YFinanceProviderAdapter:
    """
    Adapter between internal FetchRequest contracts and yfinance.

    The adapter intentionally covers only the indices/oil package for now.
    TwelveData remains the source of truth for core + fx_major.
    """

    SYMBOL_MAP: dict[Instrument, str] = {
        Instrument.GER40: "^GDAXI",
        Instrument.NAS100: "^NDX",
        Instrument.SPX500: "^GSPC",
        Instrument.UKOIL: "BZ=F",
    }

    # Extra fallback tickers for symbols that are known to be unstable on Yahoo
    # for long intraday windows. The first item remains the canonical primary.
    SYMBOL_FALLBACK_MAP: dict[Instrument, tuple[str, ...]] = {
        Instrument.GER40: ("^GDAXI", "EXS1.DE", "DAX"),
    }

    # Yahoo intraday history can rate-limit or return empty frames for long
    # periods. Try shorter periods before marking the symbol as stale.
    INTRADAY_PERIOD_FALLBACKS: dict[str, tuple[str, ...]] = {
        "15m": ("60d", "30d", "10d", "7d", "5d"),
        "30m": ("60d", "30d", "10d", "7d", "5d"),
    }

    # yfinance has no native 4h interval, so H4 is built from 1h bars.
    TIMEFRAME_MAP: dict[Timeframe, tuple[str, str, bool]] = {
        Timeframe.M15: ("15m", "60d", False),
        Timeframe.M30: ("30m", "60d", False),
        Timeframe.H1: ("1h", "730d", False),
        Timeframe.H4: ("1h", "730d", True),
        Timeframe.D1: ("1d", "5y", False),
    }

    def __init__(self, config: YFinanceAdapterConfig | None = None) -> None:
        self.config = config or YFinanceAdapterConfig()

    def fetch_time_series(self, request: FetchRequest) -> dict[str, Any]:
        provider_symbols = self._map_symbol_candidates(request.instrument)
        provider_symbol = provider_symbols[0]
        provider_interval, provider_period, needs_h4_resample = self._map_timeframe(
            request.timeframe
        )

        self._debug(
            f"[DEBUG] YF request "
            f"internal_symbol={request.instrument.value} provider_symbol={provider_symbol} "
            f"internal_tf={request.timeframe.value} provider_interval={provider_interval} "
            f"period={provider_period} symbol_candidates={provider_symbols}"
        )

        download_result = self._download_history_with_fallback(
            provider_symbols=provider_symbols,
            provider_interval=provider_interval,
            provider_period=provider_period,
        )
        df = download_result["df"]
        provider_symbol = str(download_result["provider_symbol"])
        provider_period = str(download_result["provider_period"])
        provider_fallback_used = bool(download_result["provider_fallback_used"])
        provider_period_candidates = tuple(download_result["provider_period_candidates"])
        provider_download_attempts = list(download_result["download_attempts"])

        if needs_h4_resample:
            df = self._resample_to_h4(df)

        df = self._normalize_dataframe(df)

        if df.empty:
            raise YFinanceResponseError(
                f"No usable yfinance OHLC data for symbol={provider_symbol}, "
                f"interval={provider_interval}, period={provider_period}"
            )

        if request.outputsize and request.outputsize > 0:
            df = df.tail(int(request.outputsize))

        values = self._dataframe_to_values(df)

        normalized_meta = {
            "requested_internal_symbol": request.instrument.value,
            "requested_internal_timeframe": request.timeframe.value,
            "requested_outputsize": request.outputsize,
            "provider": "yfinance",
            "provider_adapter_version": YFINANCE_PROVIDER_ADAPTER_VERSION,
            "provider_symbol": provider_symbol,
            "provider_interval": provider_interval,
            "provider_period": provider_period,
            "provider_requested_symbol": provider_symbols[0],
            "provider_symbol_candidates": list(provider_symbols),
            "provider_period_candidates": list(provider_period_candidates),
            "provider_fallback_used": provider_fallback_used,
            "provider_download_attempts": provider_download_attempts,
            "provider_resampled_to": "4h" if needs_h4_resample else None,
            "exchange": None,
            "type": "Index/Commodity",
            "timezone": self._extract_timezone(df),
            "fetched_at_utc": datetime.now(UTC).isoformat(),
        }

        payload = {
            "meta": normalized_meta,
            "values": values,
        }

        self._print_response_debug(payload)
        return payload

    def _map_symbol(self, instrument: Instrument) -> str:
        try:
            return self.SYMBOL_MAP[instrument]
        except KeyError as error:
            raise YFinanceAdapterError(
                f"No yfinance symbol mapping for {instrument.value}. "
                "This instrument may require TwelveData or another provider."
            ) from error

    def _map_timeframe(self, timeframe: Timeframe) -> tuple[str, str, bool]:
        try:
            return self.TIMEFRAME_MAP[timeframe]
        except KeyError as error:
            raise YFinanceAdapterError(
                f"No yfinance timeframe mapping for {timeframe.value}"
            ) from error

    def _map_symbol_candidates(self, instrument: Instrument) -> tuple[str, ...]:
        primary = self._map_symbol(instrument)
        configured = self.SYMBOL_FALLBACK_MAP.get(instrument)
        if not configured:
            return (primary,)
        return tuple(dict.fromkeys((primary, *configured)))

    def _period_candidates(self, *, provider_interval: str, provider_period: str) -> tuple[str, ...]:
        configured = self.INTRADAY_PERIOD_FALLBACKS.get(provider_interval)
        if not configured:
            return (provider_period,)
        return tuple(dict.fromkeys((provider_period, *configured)))

    def _download_history_with_fallback(
        self,
        *,
        provider_symbols: tuple[str, ...],
        provider_interval: str,
        provider_period: str,
    ) -> dict[str, Any]:
        period_candidates = self._period_candidates(
            provider_interval=provider_interval,
            provider_period=provider_period,
        )

        attempts: list[dict[str, Any]] = []
        last_error: Exception | None = None

        for symbol in provider_symbols:
            for period in period_candidates:
                attempt = {
                    "provider_symbol": symbol,
                    "provider_interval": provider_interval,
                    "provider_period": period,
                    "rows": 0,
                    "error_type": None,
                    "error": None,
                }

                try:
                    df = self._download_history(
                        provider_symbol=symbol,
                        provider_interval=provider_interval,
                        provider_period=period,
                    )
                    normalized = self._normalize_dataframe(df)
                    rows = int(len(normalized))
                    attempt["rows"] = rows

                    if rows > 0:
                        attempts.append(attempt)
                        fallback_used = symbol != provider_symbols[0] or period != provider_period
                        return {
                            "df": df,
                            "provider_symbol": symbol,
                            "provider_period": period,
                            "provider_fallback_used": fallback_used,
                            "provider_period_candidates": period_candidates,
                            "download_attempts": attempts,
                        }

                    attempt["error_type"] = "EMPTY_AFTER_NORMALIZATION"
                    attempt["error"] = "yfinance returned no usable OHLC rows after normalization"

                except Exception as exc:
                    last_error = exc
                    attempt["error_type"] = type(exc).__name__
                    attempt["error"] = str(exc)[:500]

                attempts.append(attempt)

        summary = "; ".join(
            f"{x.get('provider_symbol')} {x.get('provider_interval')} {x.get('provider_period')} "
            f"rows={x.get('rows')} error={x.get('error_type')}"
            for x in attempts
        )

        message = (
            "No usable yfinance OHLC data after fallback attempts. "
            f"interval={provider_interval}; attempts={summary}"
        )

        if last_error is not None:
            raise YFinanceResponseError(message) from last_error

        raise YFinanceResponseError(message)


    def _download_history(
        self,
        *,
        provider_symbol: str,
        provider_interval: str,
        provider_period: str,
    ) -> pd.DataFrame:
        try:
            import yfinance as yf
        except Exception as error:  # noqa: BLE001
            raise YFinanceAdapterError(
                "yfinance is not installed. Add 'yfinance' to requirements.txt "
                "and redeploy."
            ) from error

        try:
            df = yf.download(
                tickers=provider_symbol,
                period=provider_period,
                interval=provider_interval,
                progress=False,
                auto_adjust=self.config.auto_adjust,
                threads=False,
                timeout=self.config.timeout_seconds,
                group_by="column",
            )
        except Exception as error:  # noqa: BLE001
            raise YFinanceAdapterError(
                f"yfinance download failed for symbol={provider_symbol}, "
                f"interval={provider_interval}, period={provider_period}: {error}"
            ) from error

        if not isinstance(df, pd.DataFrame):
            raise YFinanceResponseError(
                f"Non-DataFrame yfinance response for symbol={provider_symbol}"
            )

        return df

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        normalized = df.copy()

        if isinstance(normalized.columns, pd.MultiIndex):
            # yfinance can return single-ticker data with MultiIndex columns.
            # Keep the first matching OHLCV level and drop ticker-level noise.
            if "Open" in normalized.columns.get_level_values(0):
                normalized.columns = normalized.columns.get_level_values(0)
            else:
                normalized.columns = normalized.columns.get_level_values(-1)

        rename_map = {
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
        normalized = normalized.rename(columns=rename_map)

        required = ["open", "high", "low", "close"]
        missing = [col for col in required if col not in normalized.columns]
        if missing:
            raise YFinanceResponseError(
                f"yfinance dataframe missing OHLC columns: {missing}"
            )

        if "volume" not in normalized.columns:
            normalized["volume"] = 0.0

        normalized = normalized[required + ["volume"]]
        normalized = normalized.dropna(subset=required)
        normalized = normalized.sort_index()
        return normalized

    def _resample_to_h4(self, df: pd.DataFrame) -> pd.DataFrame:
        normalized = self._normalize_dataframe(df)
        if normalized.empty:
            return normalized

        if not isinstance(normalized.index, pd.DatetimeIndex):
            normalized.index = pd.to_datetime(normalized.index, errors="coerce")
            normalized = normalized[normalized.index.notna()]

        if normalized.empty:
            return normalized

        result = normalized.resample("4h", label="right", closed="right").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        result = result.dropna(subset=["open", "high", "low", "close"])
        return result

    def _dataframe_to_values(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []

        # Match TwelveData-style latest-first ordering.
        for ts, row in df.sort_index(ascending=False).iterrows():
            values.append(
                {
                    "datetime": self._format_datetime(ts),
                    "open": self._to_float(row.get("open")),
                    "high": self._to_float(row.get("high")),
                    "low": self._to_float(row.get("low")),
                    "close": self._to_float(row.get("close")),
                    "volume": self._to_float(row.get("volume"), default=0.0),
                }
            )

        return values

    @staticmethod
    def _format_datetime(value: Any) -> str | None:
        if value is None:
            return None
        try:
            if hasattr(value, "isoformat"):
                return value.isoformat()
            return str(value)
        except Exception:
            return str(value)

    @staticmethod
    def _extract_timezone(df: pd.DataFrame) -> str | None:
        try:
            tz = getattr(df.index, "tz", None)
            return str(tz) if tz is not None else None
        except Exception:
            return None

    @staticmethod
    def _to_float(value: Any, default: float | None = None) -> float | None:
        if value is None:
            return default
        try:
            if pd.isna(value):
                return default
        except Exception:
            pass
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _print_response_debug(self, payload: dict[str, Any]) -> None:
        meta = payload.get("meta", {})
        values = payload.get("values", [])

        self._debug(f"[DEBUG] YF meta={meta}")

        if not values:
            self._debug("[DEBUG] YF response has no values")
            return

        latest_bar = values[0]
        self._debug(
            f"[DEBUG] YF response provider_symbol={meta.get('provider_symbol')} "
            f"interval={meta.get('provider_interval')} "
            f"datetime={latest_bar.get('datetime')} "
            f"close={latest_bar.get('close')} rows={len(values)}"
        )

    def _debug(self, message: str) -> None:
        if self.config.debug:
            print(message)
