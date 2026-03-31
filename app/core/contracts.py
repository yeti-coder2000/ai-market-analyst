from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

from app.core.enums import Instrument, Timeframe


@dataclass(frozen=True, slots=True)
class FetchRequest:
    instrument: Instrument
    timeframe: Timeframe
    outputsize: int = 500


@dataclass(slots=True)
class LoadResult:
    instrument: Instrument
    timeframe: Timeframe
    df: pd.DataFrame
    source: Literal["cache", "api"]
    rows: int
    last_ts: pd.Timestamp | None
    last_close: float | None