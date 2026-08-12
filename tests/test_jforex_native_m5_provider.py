from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from app.services.research.dukascopy_max_depth_provider import (
    DukascopyMaxDepthProvider,
)
from app.services.research.historical_m5_provider import HistoricalM5Request
from app.services.research.jforex_native_m5_provider import (
    CANONICAL_SYMBOL_MAPPING,
    CANONICAL_SYMBOLS,
    CHUNK_COLUMNS,
    EXPECTED_FIRST_M5_UTC,
    EXPORTER_ID,
    OFFER_SIDE,
    PERIOD,
    PROFILE_NAME,
    SCHEMA_VERSION,
    JForexNativeM5Provider,
    JForexNativeM5ProviderError,
    qualifies_for_canonical_native_parity,
)
from scripts.run_canonical_max_depth_universe_v2 import main


class JForexNativeM5ProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
        self.provider = JForexNativeM5Provider(now_utc=lambda: self.now)
        self.request = HistoricalM5Request(
            symbol="EURUSD",
            start_utc=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            end_utc=datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
            data_cutoff_utc=datetime(2026, 1, 1, 0, 10, tzinfo=UTC),
            cache_root=self.root,
        )

    @staticmethod
    def _iso(value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def _write_cache(
        self,
        request: HistoricalM5Request | None = None,
        *,
        rows: list[str] | None = None,
        manifest_overrides: dict[str, object] | None = None,
        chunk_overrides: dict[str, object] | None = None,
    ) -> tuple[Path, Path, dict[str, object]]:
        request = request or self.request
        partition = self.provider.plan(request)[0]
        partition.cache_path.parent.mkdir(parents=True, exist_ok=True)
        source_rows = rows or [
            "2026-01-01T00:00:00Z,1.10,1.20,1.00,1.15,2.5",
            "2026-01-01T00:05:00Z,1.15,1.25,1.05,1.20,3.0",
        ]
        payload = ",".join(CHUNK_COLUMNS) + "\n" + "\n".join(source_rows) + "\n"
        partition.cache_path.write_text(payload, encoding="utf-8")
        chunk: dict[str, object] = {
            "chunk_id": partition.partition_id,
            "canonical_symbol": partition.canonical_symbol,
            "provider_symbol": partition.provider_symbol,
            "relative_path": f"completed/{partition.cache_path.name}",
            "start_utc": self._iso(partition.start_utc),
            "end_exclusive_utc": self._iso(partition.end_utc),
            "first_bar_utc": source_rows[0].split(",", 1)[0],
            "last_bar_utc": source_rows[-1].split(",", 1)[0],
            "row_count": len(source_rows),
            "sha256": hashlib.sha256(partition.cache_path.read_bytes()).hexdigest(),
            "status": "COMPLETED",
            "complete": True,
        }
        chunk.update(chunk_overrides or {})
        planned = self.provider.plan(request)
        manifest: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "exporter_id": EXPORTER_ID,
            "provider_profile": PROFILE_NAME,
            "canonical_symbol": request.symbol,
            "provider_symbol": CANONICAL_SYMBOL_MAPPING[request.symbol],
            "period": PERIOD,
            "offer_side": OFFER_SIDE,
            "requested_start_utc": self._iso(planned[0].start_utc),
            "requested_end_exclusive_utc": self._iso(planned[-1].end_utc),
            "complete": True,
            "chunks": [chunk],
        }
        manifest.update(manifest_overrides or {})
        manifest_path = self.root / request.symbol / "source_manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return manifest_path, partition.cache_path, manifest

    def _rewrite_manifest(self, path: Path, manifest: dict[str, object]) -> None:
        path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    def test_exact_mapping_profile_and_native_provenance(self) -> None:
        self.assertEqual(
            CANONICAL_SYMBOL_MAPPING,
            {
                "XAUUSD": "XAU/USD",
                "BTCUSD": "BTC/USD",
                "ETHUSD": "ETH/USD",
                "EURUSD": "EUR/USD",
                "GBPUSD": "GBP/USD",
                "USDJPY": "USD/JPY",
                "USDCHF": "USD/CHF",
                "USDCAD": "USD/CAD",
                "AUDUSD": "AUD/USD",
                "GER40": "DEU.IDX/EUR",
                "NAS100": "USATECH.IDX/USD",
                "SPX500": "USA500.IDX/USD",
                "UKOIL": "BRENT.CMD/USD",
            },
        )
        self.assertEqual(tuple(CANONICAL_SYMBOL_MAPPING), CANONICAL_SYMBOLS)
        self.assertEqual(self.provider.profile_name, "JFOREX_NATIVE_M5_BID")
        self.assertEqual(self.provider.policy.source_granularity, "M5")
        self.assertEqual(self.provider.policy.m5_construction_policy, "NATIVE_PERIOD_FIVE_MINS_NO_AGGREGATION")
        self.assertEqual((PERIOD, OFFER_SIDE), ("FIVE_MINS", "BID"))
        self.assertEqual(
            {symbol: self._iso(value) for symbol, value in EXPECTED_FIRST_M5_UTC.items()},
            {
                "XAUUSD": "2003-05-05T00:00:00Z",
                "BTCUSD": "2017-05-07T23:55:00Z",
                "ETHUSD": "2017-12-11T23:50:00Z",
                "EURUSD": "2003-05-04T21:00:00Z",
                "GBPUSD": "2003-05-04T21:00:00Z",
                "USDJPY": "2003-05-04T21:00:00Z",
                "USDCHF": "2003-05-04T21:00:00Z",
                "USDCAD": "2003-08-03T21:00:00Z",
                "AUDUSD": "2003-08-03T21:00:00Z",
                "GER40": "2013-09-30T15:10:00Z",
                "NAS100": "2011-09-19T13:30:00Z",
                "SPX500": "2011-09-19T06:30:00Z",
                "UKOIL": "2010-12-02T01:00:00Z",
            },
        )

    def test_eth_native_boundary_is_not_quality_trimmed(self) -> None:
        request = HistoricalM5Request(
            "ETHUSD",
            datetime(2017, 1, 1, tzinfo=UTC),
            datetime(2017, 12, 31, 23, 59, 59, tzinfo=UTC),
            datetime(2018, 1, 1, tzinfo=UTC),
            self.root,
        )
        self.assertEqual(
            self.provider.plan(request)[0].start_utc,
            datetime(2017, 12, 11, 23, 50, tzinfo=UTC),
        )
        self.assertEqual(
            EXPECTED_FIRST_M5_UTC["ETHUSD"],
            datetime(2017, 12, 11, 23, 50, tzinfo=UTC),
        )

    def test_verified_completed_manifest_loads_without_repair(self) -> None:
        self._write_cache()
        audit = self.provider.verify_cache(self.request)
        history = self.provider.load_history(self.request)
        self.assertTrue(audit["cache_complete"])
        self.assertEqual(audit["row_count"], 2)
        self.assertEqual(audit["duplicate_timestamps"], 0)
        self.assertEqual(history["open"].tolist(), [1.10, 1.15])

    def test_missing_invalid_and_required_chunk_manifests_fail_closed(self) -> None:
        with self.assertRaisesRegex(JForexNativeM5ProviderError, "MANIFEST_MISSING"):
            self.provider.verify_cache(self.request)
        manifest_path = self.root / self.request.symbol / "source_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text("{not-json", encoding="utf-8")
        with self.assertRaisesRegex(JForexNativeM5ProviderError, "MANIFEST_INVALID"):
            self.provider.verify_cache(self.request)

        spanning = HistoricalM5Request(
            "EURUSD",
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 2, 1, 0, 5, tzinfo=UTC),
            datetime(2026, 2, 1, 0, 10, tzinfo=UTC),
            self.root,
        )
        self._write_cache(spanning)
        with self.assertRaisesRegex(JForexNativeM5ProviderError, "PARTITION_MISMATCH"):
            self.provider.verify_cache(spanning)

    def test_wrong_profile_and_symbol_mapping_fail(self) -> None:
        for overrides, reason in (
            ({"provider_profile": "TICK_AGGREGATED_M5_NON_PARITY"}, "provider_profile"),
            ({"provider_symbol": "EURUSD"}, "provider_symbol"),
        ):
            with self.subTest(reason=reason):
                manifest_path, _, manifest = self._write_cache(
                    manifest_overrides=overrides
                )
                with self.assertRaisesRegex(JForexNativeM5ProviderError, reason):
                    self.provider.verify_cache(self.request)
                manifest_path.unlink()

    def test_sha_and_row_count_mismatch_fail(self) -> None:
        with self.subTest("sha"):
            self._write_cache(chunk_overrides={"sha256": "0" * 64})
            with self.assertRaisesRegex(JForexNativeM5ProviderError, "SHA256"):
                self.provider.verify_cache(self.request)
        with self.subTest("rows"):
            self._write_cache(chunk_overrides={"row_count": 99})
            with self.assertRaisesRegex(JForexNativeM5ProviderError, "ROW_COUNT"):
                self.provider.verify_cache(self.request)

    def test_malformed_duplicate_and_non_monotonic_timestamps_fail(self) -> None:
        cases = (
            (
                [
                    "not-a-time,1,2,0,1,1",
                    "2026-01-01T00:05:00Z,1,2,0,1,1",
                ],
                "MALFORMED_TIMESTAMP",
            ),
            (
                [
                    "2026-01-01T00:00:00Z,1,2,0,1,1",
                    "2026-01-01T00:00:00Z,1,2,0,1,1",
                ],
                "DUPLICATE_TIMESTAMP",
            ),
            (
                [
                    "2026-01-01T00:05:00Z,1,2,0,1,1",
                    "2026-01-01T00:00:00Z,1,2,0,1,1",
                ],
                "NON_MONOTONIC_TIMESTAMP",
            ),
        )
        for rows, reason in cases:
            with self.subTest(reason=reason):
                self._write_cache(rows=rows)
                with self.assertRaisesRegex(JForexNativeM5ProviderError, reason):
                    self.provider.verify_cache(self.request)

    def test_ohlc_envelope_violations_fail(self) -> None:
        for row in (
            "2026-01-01T00:00:00Z,2,1,0,1,1",
            "2026-01-01T00:00:00Z,1,2,1.5,1,1",
            "2026-01-01T00:00:00Z,1,0,1,1,1",
        ):
            with self.subTest(row=row):
                self._write_cache(rows=[row])
                with self.assertRaisesRegex(JForexNativeM5ProviderError, "OHLC"):
                    self.provider.verify_cache(self.request)

    def test_current_m5_and_partial_or_incomplete_chunks_fail(self) -> None:
        current_provider = JForexNativeM5Provider(
            now_utc=lambda: datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
        )
        self._write_cache()
        with self.assertRaisesRegex(JForexNativeM5ProviderError, "IN_PROGRESS"):
            current_provider.verify_cache(self.request)

        self._write_cache(chunk_overrides={"complete": False})
        with self.assertRaisesRegex(JForexNativeM5ProviderError, "INCOMPLETE_CHUNK"):
            self.provider.verify_cache(self.request)

        self._write_cache(chunk_overrides={"relative_path": ".partial/source.csv.part"})
        with self.assertRaisesRegex(JForexNativeM5ProviderError, "PARTIAL"):
            self.provider.verify_cache(self.request)

    def test_resume_contract_accepts_only_verified_completed_chunks(self) -> None:
        self._write_cache()
        self.assertTrue(self.provider.verify_cache(self.request)["cache_complete"])
        self._write_cache(manifest_overrides={"complete": False})
        with self.assertRaisesRegex(JForexNativeM5ProviderError, "complete"):
            self.provider.verify_cache(self.request)

    def test_full_universe_gate_is_exact_and_tick_profile_never_qualifies(self) -> None:
        wrong_order = tuple(reversed(CANONICAL_SYMBOLS))
        requests = [
            HistoricalM5Request(
                symbol,
                EXPECTED_FIRST_M5_UTC[symbol],
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 1, tzinfo=UTC),
                self.root,
            )
            for symbol in wrong_order
        ]
        with self.assertRaisesRegex(JForexNativeM5ProviderError, "EXACT_CANONICAL"):
            self.provider.verify_universe(requests, require_full_universe=True)
        self.assertFalse(
            qualifies_for_canonical_native_parity(
                DukascopyMaxDepthProvider(), CANONICAL_SYMBOLS
            )
        )
        self.assertTrue(
            qualifies_for_canonical_native_parity(self.provider, CANONICAL_SYMBOLS)
        )

    def test_canonical_runner_plan_only_accepts_native_profile(self) -> None:
        output = self.root / "plan"
        arguments = [
            "--output-root",
            str(output),
            "--cache-root",
            str(self.root / "cache"),
            "--provider-profile",
            PROFILE_NAME,
            "--symbols",
            "ETHUSD",
            "--start-date",
            "2017-12-01",
            "--end-date",
            "2017-12-31",
            "--data-cutoff-utc",
            "2018-01-01T00:00:00Z",
            "--plan-only",
        ]
        with patch(
            "scripts.run_canonical_max_depth_universe_v2.check_environment",
            return_value={"versions": {"python": "3.12"}},
        ):
            self.assertEqual(main(arguments), 0)
        plan = json.loads((output / "audit_manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(plan["provider_profile"], PROFILE_NAME)
        self.assertEqual(plan["source_policy"]["source_granularity"], "M5")

    def test_generated_fixture_outputs_do_not_contain_credentials(self) -> None:
        self._write_cache()
        forbidden = ("example-password", "example-user", "DUKASCOPY_DEMO_PASSWORD=")
        for path in self.root.rglob("*"):
            if path.is_file():
                text = path.read_text(encoding="utf-8")
                self.assertFalse(any(value in text for value in forbidden))


if __name__ == "__main__":
    unittest.main()
