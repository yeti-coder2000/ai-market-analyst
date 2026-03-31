from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.core.settings import PROCESSED_DIR
from app.core.enums import Instrument, Timeframe

OHLCV_COLUMNS = ["open", "high", "low", "close", "volume"]


class ParquetCache:
    def __init__(self, base_dir: Path | str | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else PROCESSED_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _instrument_dir(self, instrument: Instrument) -> Path:
        path = self.base_dir / instrument.value
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _file_path(self, instrument: Instrument, timeframe: Timeframe) -> Path:
        return self._instrument_dir(instrument) / f"{timeframe.value}.parquet"

    def exists(self, instrument: Instrument, timeframe: Timeframe) -> bool:
        return self._file_path(instrument, timeframe).exists()

    def load(self, instrument: Instrument, timeframe: Timeframe) -> pd.DataFrame:
        path = self._file_path(instrument, timeframe)
        if not path.exists():
            return self._empty_df()

        df = pd.read_parquet(path)

        if not isinstance(df.index, pd.DatetimeIndex):
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
                df = df.set_index("datetime")
            elif "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
                df = df.set_index("timestamp")
            else:
                raise ValueError(f"Parquet file {path} has no DatetimeIndex/datetime/timestamp column")

        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
        df = df[~df.index.isna()]

        for column in OHLCV_COLUMNS:
            if column not in df.columns:
                df[column] = 0.0 if column == "volume" else pd.NA

        for column in OHLCV_COLUMNS:
            df[column] = pd.to_numeric(df[column], errors="coerce")

        df = df.dropna(subset=["open", "high", "low", "close"])
        df = df.sort_index()
        df = df[~df.index.duplicated(keep="last")]

        return df[OHLCV_COLUMNS]

    def save(self, instrument: Instrument, timeframe: Timeframe, df: pd.DataFrame) -> Path:
        normalized = self._normalize_df(df)
        path = self._file_path(instrument, timeframe)
        normalized.to_parquet(path)
        return path

    def merge_and_save(
        self,
        instrument: Instrument,
        timeframe: Timeframe,
        new_df: pd.DataFrame,
    ) -> pd.DataFrame:
        old_df = self.load(instrument, timeframe)
        merged = self.merge(old_df, new_df)
        self.save(instrument, timeframe, merged)
        return merged

    def merge(self, old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        if old_df is None or old_df.empty:
            return self._normalize_df(new_df)

        if new_df is None or new_df.empty:
            return self._normalize_df(old_df)

        merged = pd.concat([old_df, new_df])
        merged = merged.sort_index()
        merged = merged[~merged.index.duplicated(keep="last")]
        return self._normalize_df(merged)

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return self._empty_df()

        out = df.copy()

        if not isinstance(out.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be DatetimeIndex before saving")

        out.index = pd.to_datetime(out.index, utc=True, errors="coerce")
        out = out[~out.index.isna()]

        for column in OHLCV_COLUMNS:
            if column not in out.columns:
                out[column] = 0.0 if column == "volume" else pd.NA

        for column in OHLCV_COLUMNS:
            out[column] = pd.to_numeric(out[column], errors="coerce")

        out = out.dropna(subset=["open", "high", "low", "close"])
        out = out.sort_index()
        out = out[~out.index.duplicated(keep="last")]

        return out[OHLCV_COLUMNS]

    @staticmethod
    def _empty_df() -> pd.DataFrame:
        df = pd.DataFrame(columns=OHLCV_COLUMNS)
        df.index = pd.DatetimeIndex([], tz="UTC", name="datetime")
        return df