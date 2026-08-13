from __future__ import annotations

import base64
import hashlib
import json
import lzma
import struct
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from app.services.research.dukascopy_max_depth_provider import (
    PROFILE_NAME,
    DukascopyMaxDepthProvider,
    DukascopyProviderError,
)
from app.services.research.historical_m5_provider import HistoricalM5Request


class DukascopyMaxDepthProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.provider = DukascopyMaxDepthProvider()
        self.fixtures = Path(__file__).parent / "fixtures" / "dukascopy"
        self.fixture_metadata = json.loads(
            (self.fixtures / "expected.json").read_text(encoding="utf-8")
        )["fixtures"]
        self.request = HistoricalM5Request(
            "EURUSD",
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 2, tzinfo=UTC),
            self.root,
        )

    def _write(
        self, path: Path, records: list[tuple[int, int, int, float, float]]
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = b"".join(struct.pack(">3I2f", *record) for record in records)
        path.write_bytes(lzma.compress(raw, format=lzma.FORMAT_ALONE))

    def _fixture_bytes(self, name: str) -> bytes:
        fixture_name = f"{name}.b64"
        encoded = (self.fixtures / fixture_name).read_text(encoding="utf-8")
        encoded_payload = encoded.removesuffix("\n")
        self.assertEqual(encoded, encoded_payload + "\n")
        payload = base64.b64decode(encoded_payload, validate=True)
        expected = self.fixture_metadata[fixture_name]
        self.assertEqual(len(payload), expected["decoded_byte_count"])
        self.assertEqual(
            hashlib.sha256(payload).hexdigest(),
            expected["decoded_sha256"],
        )
        return payload

    def _write_source_manifest(self, hashes: dict[str, str]) -> None:
        partitions = {
            item.partition_id: {"source_sha256": hashes.get(item.partition_id)}
            for item in self.provider.plan(self.request)
        }
        (self.root / "source_hash_manifest.json").write_text(
            json.dumps(
                {
                    "profile_hash": self.provider.profile_hash(),
                    "decoder_version": self.provider.policy.decoder_version,
                    "partitions": partitions,
                }
            ),
            encoding="utf-8",
        )

    def test_profile_is_explicitly_tick_aggregated_non_parity(self) -> None:
        self.assertEqual(self.provider.profile_name, PROFILE_NAME)
        self.assertEqual(self.provider.policy.source_granularity, "TICK")
        self.assertIn("NON_PARITY", self.provider.policy.parity_status)
        self.assertEqual(self.provider.policy.price_policy, "BID")

    def test_plan_is_deterministic_and_canonical(self) -> None:
        first = self.provider.plan(self.request)
        second = self.provider.plan(self.request)
        self.assertEqual(first, second)
        self.assertEqual(
            [item.partition_id for item in first],
            ["EURUSD/2026/01/01/00", "EURUSD/2026/01/01/01"],
        )

    def test_real_format_fixture_decodes_bid_ticks_to_m5(self) -> None:
        partition = self.provider.plan(self.request)[0]
        partition.cache_path.parent.mkdir(parents=True, exist_ok=True)
        partition.cache_path.write_bytes(
            self._fixture_bytes("synthetic_real_format_ticks.bi5")
        )
        frame = self.provider.decode_partition(partition)
        self.assertEqual(len(frame), 2)
        self.assertAlmostEqual(frame.iloc[0]["open"], 1.1)
        self.assertAlmostEqual(frame.iloc[0]["high"], 1.10005)
        self.assertAlmostEqual(frame.iloc[0]["low"], 1.0999)
        self.assertEqual(frame.iloc[0]["volume"], 9.0)

    def test_duplicate_and_corrupt_fixtures_fail_closed(self) -> None:
        partition = self.provider.plan(self.request)[0]
        partition.cache_path.parent.mkdir(parents=True, exist_ok=True)
        partition.cache_path.write_bytes(self._fixture_bytes("duplicate_ticks.bi5"))
        with self.assertRaisesRegex(DukascopyProviderError, "duplicate"):
            self.provider.decode_partition(partition)
        partition.cache_path.write_bytes(
            self._fixture_bytes("corrupt_truncated_ticks.bi5")
        )
        with self.assertRaisesRegex(DukascopyProviderError, "invalid BI5"):
            self.provider.decode_partition(partition)

    def test_all_base64_fixtures_match_declared_bytes_and_hashes(self) -> None:
        for fixture_name in sorted(self.fixture_metadata):
            with self.subTest(fixture=fixture_name):
                self._fixture_bytes(fixture_name.removesuffix(".b64"))

    def test_verification_is_offline_and_reports_missing_partition(self) -> None:
        first = self.provider.plan(self.request)[0]
        self._write(first.cache_path, [(0, 110010, 110000, 1.0, 2.0)])
        self._write_source_manifest(
            {
                first.partition_id: hashlib.sha256(
                    first.cache_path.read_bytes()
                ).hexdigest()
            }
        )
        audit = self.provider.verify_cache(self.request)
        self.assertFalse(audit["cache_complete"])
        self.assertEqual(audit["partitions"][1]["status"], "MISSING")

    def test_network_implementation_is_blocked_until_native_m5_is_proved(self) -> None:
        with self.assertRaisesRegex(DukascopyProviderError, "native Dukascopy"):
            self.provider.download()

    def test_cache_verification_requires_persisted_hash_manifest(self) -> None:
        with self.assertRaisesRegex(
            DukascopyProviderError, "SOURCE_HASH_MANIFEST_MISSING"
        ):
            self.provider.verify_cache(self.request)

    def test_unverified_symbol_price_scale_fails_closed(self) -> None:
        request = HistoricalM5Request(
            "XAUUSD",
            self.request.start_utc,
            self.request.end_utc,
            self.request.data_cutoff_utc,
            self.root,
        )
        with self.assertRaisesRegex(DukascopyProviderError, "UNVERIFIED_PRICE_SCALE"):
            self.provider.decode_partition(self.provider.plan(request)[0])


if __name__ == "__main__":
    unittest.main()
