from __future__ import annotations

"""Fail-closed offline reader for native JForex M5 BID exports.

The Java exporter is the only network boundary.  This provider never contacts
Dukascopy and never falls back to BI5 ticks, price scaling, aggregation, or
source-data repair.
"""

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Callable

import pandas as pd

from app.services.research.historical_m5_provider import (
    HistoricalM5Request,
    SourcePartition,
    SourcePolicy,
)

PROFILE_NAME = "JFOREX_NATIVE_M5_BID"
SCHEMA_VERSION = "jforex-native-m5-source-manifest-v1"
EXPORTER_ID = "ai-market-analyst-jforex-native-m5-exporter-v1"
PERIOD = "FIVE_MINS"
OFFER_SIDE = "BID"
CHUNK_COLUMNS = ("timestamp_utc", "open", "high", "low", "close", "volume")

CANONICAL_SYMBOL_MAPPING: dict[str, str] = {
    "XAUUSD": "XAU/USD",
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "USDCHF": "USD/CHF",
    "USDCAD": "USD/CAD",
    "AUDUSD": "AUD/USD",
    "BTCUSD": "BTC/USD",
    "ETHUSD": "ETH/USD",
    "GER40": "DEU.IDX/EUR",
    "NAS100": "USATECH.IDX/USD",
    "SPX500": "USA500.IDX/USD",
    "UKOIL": "BRENT.CMD/USD",
}
CANONICAL_SYMBOLS = tuple(CANONICAL_SYMBOL_MAPPING)

EXPECTED_FIRST_M5_UTC: dict[str, datetime] = {
    "XAUUSD": datetime(2003, 5, 5, 0, 0, tzinfo=UTC),
    "BTCUSD": datetime(2017, 5, 7, 23, 55, tzinfo=UTC),
    "ETHUSD": datetime(2017, 12, 11, 23, 50, tzinfo=UTC),
    "EURUSD": datetime(2003, 5, 4, 21, 0, tzinfo=UTC),
    "GBPUSD": datetime(2003, 5, 4, 21, 0, tzinfo=UTC),
    "USDJPY": datetime(2003, 5, 4, 21, 0, tzinfo=UTC),
    "USDCHF": datetime(2003, 5, 4, 21, 0, tzinfo=UTC),
    "USDCAD": datetime(2003, 8, 3, 21, 0, tzinfo=UTC),
    "AUDUSD": datetime(2003, 8, 3, 21, 0, tzinfo=UTC),
    "GER40": datetime(2013, 9, 30, 15, 10, tzinfo=UTC),
    "NAS100": datetime(2011, 9, 19, 13, 30, tzinfo=UTC),
    "SPX500": datetime(2011, 9, 19, 6, 30, tzinfo=UTC),
    "UKOIL": datetime(2010, 12, 2, 1, 0, tzinfo=UTC),
}

SOURCE_POLICY = SourcePolicy(
    source_format="JFOREX_NATIVE_UTF8_CSV_V1",
    source_granularity="M5",
    price_policy="BID",
    m5_construction_policy="NATIVE_PERIOD_FIVE_MINS_NO_AGGREGATION",
    timezone_policy="UTC_BAR_OPEN",
    volume_semantics="JFOREX_NATIVE_BAR_VOLUME",
    compression_container="NONE",
    decoder_version="jforex-native-m5-offline-reader-v1",
    parity_status="NATIVE_JFOREX_M5_BID_PARITY_CANDIDATE",
)


class JForexNativeM5ProviderError(RuntimeError):
    """Safe fail-closed source-contract error."""


def _utc(value: Any, *, field: str) -> datetime:
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        raise JForexNativeM5ProviderError(f"MALFORMED_TIMESTAMP:{field}") from None
    if pd.isna(stamp):
        raise JForexNativeM5ProviderError(f"MALFORMED_TIMESTAMP:{field}")
    stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
    return stamp.to_pydatetime()


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _month_after(value: datetime) -> datetime:
    if value.month == 12:
        return datetime(value.year + 1, 1, 1, tzinfo=UTC)
    return datetime(value.year, value.month + 1, 1, tzinfo=UTC)


def _closed_bar_end(request: HistoricalM5Request) -> datetime:
    end = _utc(request.end_utc, field="request.end_utc")
    cutoff = _utc(request.data_cutoff_utc, field="request.data_cutoff_utc")
    floored = end.replace(
        minute=end.minute - end.minute % 5, second=0, microsecond=0
    )
    if end == floored:
        requested_exclusive = end + timedelta(minutes=5)
    else:
        requested_exclusive = floored + timedelta(minutes=5)
    return min(requested_exclusive, cutoff)


class JForexNativeM5Provider:
    """Consume only complete, hashed source chunks emitted by the Java exporter."""

    profile_name = PROFILE_NAME
    policy = SOURCE_POLICY
    symbol_mapping = CANONICAL_SYMBOL_MAPPING
    expected_first_m5_utc = EXPECTED_FIRST_M5_UTC

    def __init__(self, *, now_utc: Callable[[], datetime] | None = None) -> None:
        self._now_utc = now_utc or (lambda: datetime.now(UTC))

    @staticmethod
    def profile_hash() -> str:
        payload = repr(
            (
                PROFILE_NAME,
                SCHEMA_VERSION,
                EXPORTER_ID,
                PERIOD,
                OFFER_SIDE,
                SOURCE_POLICY.to_dict(),
                tuple(CANONICAL_SYMBOL_MAPPING.items()),
                tuple((key, _iso(value)) for key, value in EXPECTED_FIRST_M5_UTC.items()),
            )
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def plan(self, request: HistoricalM5Request) -> Sequence[SourcePartition]:
        symbol = request.symbol
        if symbol != symbol.upper() or symbol not in self.symbol_mapping:
            raise JForexNativeM5ProviderError("UNSUPPORTED_CANONICAL_SYMBOL")
        start = max(
            _utc(request.start_utc, field="request.start_utc"),
            self.expected_first_m5_utc[symbol],
        )
        end_exclusive = _closed_bar_end(request)
        cutoff = _utc(request.data_cutoff_utc, field="request.data_cutoff_utc")
        if end_exclusive > cutoff or start >= end_exclusive:
            raise JForexNativeM5ProviderError("INVALID_REQUEST_RANGE")
        if any(
            value.second or value.microsecond or value.minute % 5
            for value in (start, end_exclusive, cutoff)
        ):
            raise JForexNativeM5ProviderError("REQUEST_NOT_ALIGNED_TO_M5")
        rows: list[SourcePartition] = []
        cursor = start
        while cursor < end_exclusive:
            partition_end = min(_month_after(cursor), end_exclusive)
            partition_id = f"{symbol}/{cursor:%Y-%m}"
            rows.append(
                SourcePartition(
                    canonical_symbol=symbol,
                    provider_symbol=self.symbol_mapping[symbol],
                    partition_id=partition_id,
                    start_utc=cursor,
                    end_utc=partition_end,
                    cache_path=(
                        request.cache_root
                        / symbol
                        / "completed"
                        / f"{symbol}_{cursor:%Y-%m}.csv"
                    ),
                )
            )
            cursor = partition_end
        return rows

    def download(self, *_: Any, **__: Any) -> None:
        raise JForexNativeM5ProviderError(
            "NATIVE_EXPORTER_REQUIRED: run the Java 17 JForex exporter in a separate "
            "process/session, then use --replay-only"
        )

    def _manifest_path(self, request: HistoricalM5Request) -> Path:
        return request.cache_root / request.symbol / "source_manifest.json"

    def _read_manifest(self, request: HistoricalM5Request) -> tuple[Path, dict[str, Any]]:
        path = self._manifest_path(request)
        if not path.is_file():
            raise JForexNativeM5ProviderError("SOURCE_MANIFEST_MISSING")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise JForexNativeM5ProviderError("SOURCE_MANIFEST_INVALID") from None
        if not isinstance(payload, dict):
            raise JForexNativeM5ProviderError("SOURCE_MANIFEST_INVALID")
        return path, payload

    def _validate_manifest_identity(
        self,
        request: HistoricalM5Request,
        manifest: Mapping[str, Any],
    ) -> None:
        symbol = request.symbol
        required = {
            "schema_version": SCHEMA_VERSION,
            "exporter_id": EXPORTER_ID,
            "provider_profile": PROFILE_NAME,
            "canonical_symbol": symbol,
            "provider_symbol": self.symbol_mapping[symbol],
            "period": PERIOD,
            "offer_side": OFFER_SIDE,
            "complete": True,
            "requested_start_utc": _iso(
                max(
                    _utc(request.start_utc, field="request.start_utc"),
                    self.expected_first_m5_utc[symbol],
                )
            ),
            "requested_end_exclusive_utc": _iso(_closed_bar_end(request)),
        }
        for field, expected in required.items():
            if manifest.get(field) != expected:
                raise JForexNativeM5ProviderError(
                    f"SOURCE_MANIFEST_IDENTITY_MISMATCH:{field}"
                )

    def _resolve_completed_path(
        self, request: HistoricalM5Request, relative_path: Any
    ) -> Path:
        if not isinstance(relative_path, str):
            raise JForexNativeM5ProviderError("INVALID_CHUNK_PATH")
        pure = PurePosixPath(relative_path)
        if (
            pure.is_absolute()
            or ".." in pure.parts
            or not pure.parts
            or pure.parts[0] != "completed"
            or any(part.startswith(".partial") for part in pure.parts)
        ):
            raise JForexNativeM5ProviderError("PARTIAL_OR_UNSAFE_CHUNK_PATH")
        root = (request.cache_root / request.symbol).resolve()
        path = (root / Path(*pure.parts)).resolve()
        if path.parent != root / "completed" or not path.is_file() or path.is_symlink():
            raise JForexNativeM5ProviderError("COMPLETED_CHUNK_MISSING")
        return path

    @staticmethod
    def _decimal(value: Any, *, field: str) -> Decimal:
        try:
            decimal = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise JForexNativeM5ProviderError(f"INVALID_NUMERIC:{field}") from None
        if not decimal.is_finite():
            raise JForexNativeM5ProviderError(f"INVALID_NUMERIC:{field}")
        return decimal

    def _decode_chunk(
        self,
        request: HistoricalM5Request,
        partition: SourcePartition,
        chunk: Mapping[str, Any],
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        if chunk.get("status") != "COMPLETED" or chunk.get("complete") is not True:
            raise JForexNativeM5ProviderError("INCOMPLETE_CHUNK")
        if chunk.get("chunk_id") != partition.partition_id:
            raise JForexNativeM5ProviderError("CHUNK_ID_MISMATCH")
        if chunk.get("canonical_symbol") != partition.canonical_symbol:
            raise JForexNativeM5ProviderError("CHUNK_SYMBOL_MISMATCH")
        if chunk.get("provider_symbol") != partition.provider_symbol:
            raise JForexNativeM5ProviderError("CHUNK_PROVIDER_SYMBOL_MISMATCH")
        if chunk.get("start_utc") != _iso(partition.start_utc) or chunk.get(
            "end_exclusive_utc"
        ) != _iso(partition.end_utc):
            raise JForexNativeM5ProviderError("CHUNK_RANGE_MISMATCH")
        path = self._resolve_completed_path(request, chunk.get("relative_path"))
        declared_hash = chunk.get("sha256")
        actual_hash = _sha256(path)
        if not isinstance(declared_hash, str) or actual_hash != declared_hash:
            raise JForexNativeM5ProviderError("SOURCE_CHUNK_SHA256_MISMATCH")
        try:
            frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        except (OSError, UnicodeError, pd.errors.ParserError):
            raise JForexNativeM5ProviderError("SOURCE_CHUNK_CSV_INVALID") from None
        if tuple(frame.columns) != CHUNK_COLUMNS:
            raise JForexNativeM5ProviderError("SOURCE_CHUNK_SCHEMA_MISMATCH")
        declared_rows = chunk.get("row_count")
        if isinstance(declared_rows, bool) or not isinstance(declared_rows, int):
            raise JForexNativeM5ProviderError("SOURCE_CHUNK_ROW_COUNT_INVALID")
        if declared_rows <= 0 or len(frame) != declared_rows:
            raise JForexNativeM5ProviderError("SOURCE_CHUNK_ROW_COUNT_MISMATCH")
        timestamps: list[datetime] = []
        values: dict[str, list[float]] = {
            name: [] for name in CHUNK_COLUMNS if name != "timestamp_utc"
        }
        previous: datetime | None = None
        for index, row in frame.iterrows():
            raw_timestamp = row["timestamp_utc"]
            if not raw_timestamp.endswith("Z"):
                raise JForexNativeM5ProviderError("MALFORMED_TIMESTAMP:timestamp_utc")
            stamp = _utc(raw_timestamp, field="timestamp_utc")
            if stamp.second or stamp.microsecond or stamp.minute % 5:
                raise JForexNativeM5ProviderError("TIMESTAMP_NOT_ALIGNED_TO_M5")
            if not partition.start_utc <= stamp < partition.end_utc:
                raise JForexNativeM5ProviderError("TIMESTAMP_OUTSIDE_CHUNK_RANGE")
            if previous is not None and stamp <= previous:
                reason = "DUPLICATE_TIMESTAMP" if stamp == previous else "NON_MONOTONIC_TIMESTAMP"
                raise JForexNativeM5ProviderError(reason)
            previous = stamp
            timestamps.append(stamp)
            decimals = {
                field: self._decimal(row[field], field=field)
                for field in ("open", "high", "low", "close", "volume")
            }
            if (
                decimals["high"] < max(decimals["open"], decimals["close"])
                or decimals["low"] > min(decimals["open"], decimals["close"])
                or decimals["high"] < decimals["low"]
                or decimals["volume"] < 0
            ):
                raise JForexNativeM5ProviderError(f"OHLC_ENVELOPE_VIOLATION:{index}")
            for field, value in decimals.items():
                numeric = float(value)
                if not math.isfinite(numeric):
                    raise JForexNativeM5ProviderError(f"INVALID_NUMERIC:{field}")
                values[field].append(numeric)
        if chunk.get("first_bar_utc") != _iso(timestamps[0]) or chunk.get(
            "last_bar_utc"
        ) != _iso(timestamps[-1]):
            raise JForexNativeM5ProviderError("CHUNK_BAR_RANGE_MISMATCH")
        data = {"timestamp": timestamps, **values}
        decoded = pd.DataFrame(data)
        return decoded, {
            "partition_id": partition.partition_id,
            "status": "VERIFIED",
            "source_sha256": actual_hash,
            "row_count": len(decoded),
            "first_bar_utc": _iso(timestamps[0]),
            "last_bar_utc": _iso(timestamps[-1]),
            "relative_path": chunk["relative_path"],
        }

    def _verified_frames(
        self, request: HistoricalM5Request
    ) -> tuple[list[pd.DataFrame], dict[str, Any]]:
        manifest_path, manifest = self._read_manifest(request)
        self._validate_manifest_identity(request, manifest)
        chunks = manifest.get("chunks")
        if not isinstance(chunks, list) or not chunks:
            raise JForexNativeM5ProviderError("SOURCE_MANIFEST_CHUNKS_MISSING")
        by_id: dict[str, Mapping[str, Any]] = {}
        for chunk in chunks:
            if not isinstance(chunk, Mapping) or not isinstance(chunk.get("chunk_id"), str):
                raise JForexNativeM5ProviderError("SOURCE_MANIFEST_CHUNK_INVALID")
            if chunk["chunk_id"] in by_id:
                raise JForexNativeM5ProviderError("DUPLICATE_CHUNK_ID")
            by_id[chunk["chunk_id"]] = chunk
        partitions = list(self.plan(request))
        expected_ids = [partition.partition_id for partition in partitions]
        if list(by_id) != expected_ids:
            raise JForexNativeM5ProviderError("SOURCE_MANIFEST_PARTITION_MISMATCH")
        frames: list[pd.DataFrame] = []
        audits: list[dict[str, Any]] = []
        previous_last: datetime | None = None
        for partition in partitions:
            frame, audit = self._decode_chunk(request, partition, by_id[partition.partition_id])
            first = frame.iloc[0]["timestamp"]
            if previous_last is not None and first <= previous_last:
                reason = "DUPLICATE_TIMESTAMP" if first == previous_last else "NON_MONOTONIC_TIMESTAMP"
                raise JForexNativeM5ProviderError(reason)
            previous_last = frame.iloc[-1]["timestamp"]
            frames.append(frame)
            audits.append(audit)
        combined = pd.concat(frames, ignore_index=True)
        timestamps = pd.DatetimeIndex(pd.to_datetime(combined["timestamp"], utc=True))
        if timestamps.duplicated().any():
            raise JForexNativeM5ProviderError("DUPLICATE_TIMESTAMP")
        if not timestamps.is_monotonic_increasing:
            raise JForexNativeM5ProviderError("NON_MONOTONIC_TIMESTAMP")
        requested_start = _utc(request.start_utc, field="request.start_utc")
        authoritative_first = self.expected_first_m5_utc[request.symbol]
        if requested_start <= authoritative_first and (
            timestamps[0].to_pydatetime() != authoritative_first
        ):
            raise JForexNativeM5ProviderError("AUTHORITATIVE_FIRST_M5_MISSING")
        now = _utc(self._now_utc(), field="now_utc")
        current_open = now.replace(
            minute=now.minute - now.minute % 5,
            second=0,
            microsecond=0,
        )
        cutoff = min(
            _utc(request.data_cutoff_utc, field="request.data_cutoff_utc"),
            current_open,
        )
        if any(stamp.to_pydatetime() >= cutoff for stamp in timestamps):
            raise JForexNativeM5ProviderError("CURRENT_OR_IN_PROGRESS_M5_INCLUDED")
        differences = timestamps.to_series().diff().dropna()
        missing_intervals = int(
            sum(max(0, int(delta / pd.Timedelta(minutes=5)) - 1) for delta in differences)
        )
        return frames, {
            "cache_complete": True,
            "provider_profile": PROFILE_NAME,
            "canonical_symbol": request.symbol,
            "provider_symbol": self.symbol_mapping[request.symbol],
            "period": PERIOD,
            "offer_side": OFFER_SIDE,
            "manifest_sha256": _sha256(manifest_path),
            "row_count": len(combined),
            "first_bar_utc": _iso(timestamps[0].to_pydatetime()),
            "last_bar_utc": _iso(timestamps[-1].to_pydatetime()),
            "authoritative_first_m5_utc": _iso(authoritative_first),
            "missing_intervals": missing_intervals,
            "duplicate_timestamps": 0,
            "partitions": audits,
        }

    def decode_partition(self, partition: SourcePartition) -> pd.DataFrame:
        raise JForexNativeM5ProviderError(
            "MANIFEST_CONTEXT_REQUIRED: use verify_cache/load_history"
        )

    def verify_cache(self, request: HistoricalM5Request) -> dict[str, Any]:
        _, audit = self._verified_frames(request)
        return audit

    def load_history(self, request: HistoricalM5Request) -> pd.DataFrame:
        frames, _ = self._verified_frames(request)
        return pd.concat(frames, ignore_index=True)

    def verify_universe(
        self,
        requests: Sequence[HistoricalM5Request],
        *,
        require_full_universe: bool,
    ) -> dict[str, dict[str, Any]]:
        symbols = tuple(request.symbol for request in requests)
        if require_full_universe and symbols != CANONICAL_SYMBOLS:
            raise JForexNativeM5ProviderError(
                "FULL_UNIVERSE_REQUIRES_EXACT_CANONICAL_13_SYMBOL_ORDER"
            )
        return {request.symbol: self.verify_cache(request) for request in requests}


def qualifies_for_canonical_native_parity(
    provider: Any, symbols: Sequence[str]
) -> bool:
    """Return true only for the exact native profile and exact 13-symbol order."""

    return (
        getattr(provider, "profile_name", None) == PROFILE_NAME
        and getattr(provider, "policy", None) == SOURCE_POLICY
        and getattr(provider, "symbol_mapping", None) == CANONICAL_SYMBOL_MAPPING
        and tuple(symbols) == CANONICAL_SYMBOLS
    )
