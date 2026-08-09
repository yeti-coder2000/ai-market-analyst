from __future__ import annotations

"""Provider-neutral contracts for immutable research M5 inputs."""

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import pandas as pd


@dataclass(frozen=True, slots=True)
class SourcePolicy:
    source_format: str
    source_granularity: str
    price_policy: str
    m5_construction_policy: str
    timezone_policy: str
    volume_semantics: str
    compression_container: str
    decoder_version: str
    parity_status: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SourcePartition:
    canonical_symbol: str
    provider_symbol: str
    partition_id: str
    start_utc: datetime
    end_utc: datetime
    cache_path: Path


@dataclass(frozen=True, slots=True)
class HistoricalM5Request:
    symbol: str
    start_utc: datetime
    end_utc: datetime
    data_cutoff_utc: datetime
    cache_root: Path


class HistoricalM5Provider(Protocol):
    profile_name: str
    policy: SourcePolicy

    def plan(self, request: HistoricalM5Request) -> Sequence[SourcePartition]: ...

    def decode_partition(self, partition: SourcePartition) -> pd.DataFrame: ...

    def verify_cache(self, request: HistoricalM5Request) -> dict[str, Any]: ...
