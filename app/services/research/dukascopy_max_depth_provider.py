from __future__ import annotations

"""Dukascopy-format cache decoder for non-parity tick-aggregated M5 research.

The only source layout proven by committed golden fixtures is the common LZMA
``.bi5`` tick layout: big-endian ``time_ms, ask, bid, ask_volume, bid_volume``.
It is tick data, not native Dukascopy SWFX/CFD bid M5 bars.  Consequently this
profile is deliberately named ``TICK_AGGREGATED_M5_NON_PARITY`` and network
acquisition remains disabled until an authoritative native-M5 URL, format and
13-symbol mapping are independently proved.
"""

import hashlib
import json
import lzma
import struct
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import pandas as pd

from app.services.ltf_execution_backtest import normalize_m5_history
from app.services.research.historical_m5_provider import (
    HistoricalM5Request,
    SourcePartition,
    SourcePolicy,
)

PROFILE_NAME = "TICK_AGGREGATED_M5_NON_PARITY"
DECODER_VERSION = "dukascopy-bi5-tick-decoder-v1"
TICK_RECORD = struct.Struct(">3I2f")

SOURCE_POLICY = SourcePolicy(
    source_format="DUKASCOPY_BI5_TICK_RECORDS_BIG_ENDIAN_3I2F",
    source_granularity="TICK",
    price_policy="BID",
    m5_construction_policy="UTC_LEFT_CLOSED_TICK_AGGREGATION",
    timezone_policy="UTC_PARTITION_HOUR_PLUS_MILLISECOND_OFFSET",
    volume_semantics="SUM_PROVIDER_BID_VOLUME_PER_M5",
    compression_container="LZMA_ALONE_BI5",
    decoder_version=DECODER_VERSION,
    parity_status="NON_PARITY_WITH_NATIVE_SWFX_CFD_BID_M5",
)

# Mapping is intentionally diagnostic, not asserted as an authoritative native
# M5 mapping. Network use is blocked until every value is independently proved.
DIAGNOSTIC_SYMBOL_MAPPING = {
    "XAUUSD": "XAUUSD",
    "EURUSD": "EURUSD",
    "GBPUSD": "GBPUSD",
    "USDJPY": "USDJPY",
    "USDCHF": "USDCHF",
    "USDCAD": "USDCAD",
    "AUDUSD": "AUDUSD",
    "BTCUSD": "BTCUSD",
    "ETHUSD": "ETHUSD",
    "GER40": "DEUIDXEUR",
    "NAS100": "USA100IDXUSD",
    "SPX500": "USA500IDXUSD",
    "UKOIL": "BRENTCMDUSD",
}
VERIFIED_PRICE_SCALE = {"EURUSD": 100000.0}


class DukascopyProviderError(RuntimeError):
    """Safe fail-closed provider error."""


class DukascopyMaxDepthProvider:
    profile_name = PROFILE_NAME
    policy = SOURCE_POLICY
    symbol_mapping = DIAGNOSTIC_SYMBOL_MAPPING

    @staticmethod
    def profile_hash() -> str:
        payload = repr(
            (
                PROFILE_NAME,
                SOURCE_POLICY.to_dict(),
                sorted(DIAGNOSTIC_SYMBOL_MAPPING.items()),
                sorted(VERIFIED_PRICE_SCALE.items()),
            )
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def plan(self, request: HistoricalM5Request) -> Sequence[SourcePartition]:
        symbol = request.symbol.upper()
        if symbol != request.symbol or symbol not in self.symbol_mapping:
            raise DukascopyProviderError(
                f"unsupported canonical symbol={request.symbol!r}"
            )
        if request.end_utc > request.data_cutoff_utc:
            raise DukascopyProviderError("requested end exceeds data cutoff")
        cursor = request.start_utc.replace(minute=0, second=0, microsecond=0)
        end = request.end_utc.replace(minute=0, second=0, microsecond=0)
        rows: list[SourcePartition] = []
        while cursor <= end:
            partition_id = f"{symbol}/{cursor:%Y/%m/%d/%H}"
            rows.append(
                SourcePartition(
                    symbol,
                    self.symbol_mapping[symbol],
                    partition_id,
                    cursor,
                    cursor + timedelta(hours=1),
                    request.cache_root / symbol / f"{cursor:%Y/%m/%d/%H}h_ticks.bi5",
                )
            )
            cursor += timedelta(hours=1)
        return rows

    def download(self, *_: Any, **__: Any) -> None:
        raise DukascopyProviderError(
            "network acquisition blocked: authoritative native Dukascopy SWFX/CFD "
            "bid M5 source format and complete 13-symbol mapping are not proved"
        )

    def decode_partition(self, partition: SourcePartition) -> pd.DataFrame:
        price_scale = VERIFIED_PRICE_SCALE.get(partition.canonical_symbol)
        if price_scale is None:
            raise DukascopyProviderError("UNVERIFIED_PRICE_SCALE")
        try:
            raw = lzma.decompress(partition.cache_path.read_bytes())
        except (OSError, lzma.LZMAError) as error:
            raise DukascopyProviderError(
                f"invalid BI5 partition; error_type={type(error).__name__}"
            ) from None
        if not raw or len(raw) % TICK_RECORD.size:
            raise DukascopyProviderError("invalid BI5 tick record length")
        records: list[dict[str, Any]] = []
        identities: set[tuple[int, int, int]] = set()
        for offset in range(0, len(raw), TICK_RECORD.size):
            millis, ask, bid, _ask_volume, bid_volume = TICK_RECORD.unpack_from(
                raw, offset
            )
            identity = (millis, ask, bid)
            if identity in identities:
                raise DukascopyProviderError("duplicate BI5 tick identity")
            identities.add(identity)
            if millis >= 3_600_000 or ask < bid:
                raise DukascopyProviderError("invalid BI5 tick values")
            records.append(
                {
                    "timestamp": partition.start_utc + timedelta(milliseconds=millis),
                    "price": bid / price_scale,
                    "volume": float(bid_volume),
                }
            )
        ticks = pd.DataFrame(records).sort_values("timestamp")
        indexed = ticks.set_index("timestamp")
        m5 = (
            indexed.resample("5min", label="left", closed="left")
            .agg(
                open=("price", "first"),
                high=("price", "max"),
                low=("price", "min"),
                close=("price", "last"),
                volume=("volume", "sum"),
            )
            .dropna(subset=["open", "high", "low", "close"])
        )
        return normalize_m5_history(m5, symbol=partition.canonical_symbol)

    def verify_cache(self, request: HistoricalM5Request) -> dict[str, Any]:
        partitions = self.plan(request)
        manifest_path = request.cache_root / "source_hash_manifest.json"
        if not manifest_path.is_file():
            raise DukascopyProviderError("SOURCE_HASH_MANIFEST_MISSING")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("profile_hash") != self.profile_hash()
            or manifest.get("decoder_version") != DECODER_VERSION
        ):
            raise DukascopyProviderError("SOURCE_HASH_MANIFEST_PROFILE_MISMATCH")
        declared = manifest.get("partitions") or {}
        expected_ids = {item.partition_id for item in partitions}
        if set(declared) != expected_ids:
            raise DukascopyProviderError("SOURCE_HASH_MANIFEST_PARTITION_MISMATCH")
        expected_paths = {item.cache_path.resolve() for item in partitions}
        extras = {
            path.resolve()
            for path in (request.cache_root / request.symbol).rglob("*.bi5")
        } - expected_paths
        if extras:
            raise DukascopyProviderError("UNEXPECTED_SOURCE_PARTITION")
        audits = []
        complete = True
        for item in partitions:
            if not item.cache_path.is_file():
                audits.append({"partition_id": item.partition_id, "status": "MISSING"})
                complete = False
                continue
            digest = hashlib.sha256(item.cache_path.read_bytes()).hexdigest()
            if digest != declared[item.partition_id].get("source_sha256"):
                raise DukascopyProviderError("SOURCE_HASH_MISMATCH")
            frame = self.decode_partition(item)
            audits.append(
                {
                    "partition_id": item.partition_id,
                    "status": "VERIFIED",
                    "source_sha256": digest,
                    "m5_rows": len(frame),
                }
            )
        return {"cache_complete": complete, "partitions": audits}
