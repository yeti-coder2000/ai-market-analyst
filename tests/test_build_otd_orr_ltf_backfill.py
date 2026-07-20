from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from scripts.build_otd_orr_ltf_backfill import (
    BAR_COLUMNS,
    BackfillError,
    CohortIntegrityError,
    CoverageError,
    _safe_provider_error,
    build_dataset,
    extract_clean_cohort,
    normalize_ohlc_frame,
    select_replay_bars,
    sha256_file,
)


def _pickle_backed_to_parquet(
    frame: pd.DataFrame,
    path: Path | str,
    *,
    index: bool | None = None,
    **_: object,
) -> None:
    """Exercise Parquet call sites when the test runtime lacks pyarrow."""

    payload = frame.reset_index(drop=True) if index is False else frame
    payload.to_pickle(path)


def _pickle_backed_read_parquet(path: Path | str, **_: object) -> pd.DataFrame:
    return pd.read_pickle(path)


class OtdOrrLtfBackfillBuilderTest(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.read_parquet_patch = patch.object(
            pd,
            "read_parquet",
            side_effect=_pickle_backed_read_parquet,
        )
        self.write_parquet_patch = patch.object(
            pd.DataFrame,
            "to_parquet",
            new=_pickle_backed_to_parquet,
        )
        self.read_parquet_patch.start()
        self.write_parquet_patch.start()
        self.temporary = tempfile.TemporaryDirectory(prefix="otd-orr-ltf-test-")
        self.root = Path(self.temporary.name)
        self.outcomes = self.root / "stats" / "integrity-v2.json"
        self.cache = self.root / "cache"
        self.five_minute_source = self.root / "five-minute-source"
        self.output = self.root / "research" / "dataset-v0.1"

        self.row = {
            "tracking_scope": "RESEARCH_COUNTERFACTUAL",
            "signal_id": "XAUUSD_SIGNAL_001",
            "symbol": "XAUUSD",
            "scenario_family": "TPO_OPEN_TEST_DRIVE",
            "scenario_type": "TPO_OPEN_TEST_DRIVE_SHORT",
            "direction": "SHORT",
            "signal_created_at_utc": "2026-06-18T09:30:00+00:00",
            "source_event_ts_utc": "2026-06-18T10:02:00+00:00",
            "expires_at_utc": "2026-06-18T10:30:00+00:00",
            "htf_bias": "SHORT",
            "signal_alignment": "TREND_ALIGNED",
            "macro_guard_status": "MACRO_CLEAR",
            "stop_quality": "OK",
            "battle_permission": "RESEARCH_ONLY",
            "battle_permission_blockers": ["research_only"],
        }
        self._write_outcomes([self.row])
        self._write_ohlc(
            self.cache / "XAUUSD" / "15m.parquet",
            start="2026-06-18T08:00:00+00:00",
            periods=12,
            frequency="15min",
        )
        self._write_ohlc(
            self.five_minute_source / "XAUUSD" / "5m.parquet",
            start="2026-06-18T09:00:00+00:00",
            periods=20,
            frequency="5min",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()
        self.write_parquet_patch.stop()
        self.read_parquet_patch.stop()

    def _write_outcomes(self, rows: list[dict[str, object]]) -> None:
        self.outcomes.parent.mkdir(parents=True, exist_ok=True)
        self.outcomes.write_text(
            json.dumps({"version": "2.1", "records": rows}),
            encoding="utf-8",
        )

    @staticmethod
    def _ohlc_frame(
        *,
        start: str,
        periods: int,
        frequency: str,
    ) -> pd.DataFrame:
        index = pd.date_range(start=start, periods=periods, freq=frequency, tz="UTC")
        base = pd.Series(range(periods), dtype=float).to_numpy() + 100.0
        return pd.DataFrame(
            {
                "open": base,
                "high": base + 1.0,
                "low": base - 1.0,
                "close": base + 0.5,
                "volume": base * 10.0,
            },
            index=index,
        )

    def _write_ohlc(
        self,
        path: Path,
        *,
        start: str,
        periods: int,
        frequency: str,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._ohlc_frame(
            start=start,
            periods=periods,
            frequency=frequency,
        ).to_parquet(path)

    def _build(self, *, output: Path | None = None, dry_run: bool = False) -> dict[str, object]:
        return build_dataset(
            outcomes_path=self.outcomes,
            cache_dir=self.cache,
            output_dir=output or self.output,
            five_minute_source="directory",
            five_minute_source_dir=self.five_minute_source,
            expected_cohort_size=1,
            pre_event_bars=3,
            dry_run=dry_run,
        )

    def test_clean_cohort_anchors_decision_to_signal_creation_and_original_expiry(self) -> None:
        records = extract_clean_cohort(
            self.outcomes,
            expected_cohort_size=1,
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].decision_time_utc.isoformat(), "2026-06-18T09:30:00+00:00")
        self.assertEqual(
            records[0].selected_evaluation_at_utc.isoformat(),
            "2026-06-18T10:02:00+00:00",
        )
        self.assertTrue(records[0].selected_evaluation_within_original_lifecycle)
        self.assertEqual(records[0].selected_evaluation_lag_minutes, 32.0)
        self.assertIsNone(records[0].selected_evaluation_after_expiry_minutes)
        self.assertEqual(records[0].expires_at_utc.isoformat(), "2026-06-18T10:30:00+00:00")
        self.assertEqual(records[0].scenario_family, "TPO_OPEN_TEST_DRIVE")

    def test_evaluation_after_expiry_is_audited_and_never_used_as_decision(self) -> None:
        late_evaluation = {
            **self.row,
            "created_at_utc": "2026-06-18T10:45:00+00:00",
            "source_event_ts_utc": "2026-06-18T10:45:00+00:00",
        }
        self._write_outcomes([late_evaluation])

        record = extract_clean_cohort(self.outcomes, expected_cohort_size=1)[0]
        serialized = record.to_dict()

        self.assertEqual(record.decision_time_utc.isoformat(), "2026-06-18T09:30:00+00:00")
        self.assertEqual(
            record.selected_evaluation_at_utc.isoformat(),
            "2026-06-18T10:45:00+00:00",
        )
        self.assertFalse(record.selected_evaluation_within_original_lifecycle)
        self.assertEqual(record.selected_evaluation_after_expiry_minutes, 15.0)
        self.assertEqual(
            serialized["selected_evaluation_audit"]["context_use"],
            "AUDIT_ONLY_OUTSIDE_ORIGINAL_LIFECYCLE",
        )

        result = self._build(dry_run=True)
        manifest = result["manifest"]

        self.assertEqual(result["status"], "DRY_RUN_OK")
        self.assertFalse(result["output_created"])
        self.assertFalse(self.output.exists())
        self.assertEqual(
            manifest["cohort"]["selected_evaluation_audit"],
            {
                "within_original_lifecycle": 0,
                "at_or_after_original_expiry": 1,
                "context_use_rule": (
                    "selected evaluation metadata is audit-only and must not be "
                    "used before selected_evaluation_at_utc; an evaluation at or "
                    "after expiry is never replay-eligible context"
                ),
            },
        )
        self.assertEqual(
            manifest["coverage"][0]["decision_time_utc"],
            "2026-06-18T09:30:00+00:00",
        )
        self.assertTrue(manifest["coverage"][0]["coverage_complete"])

    def test_duplicate_signal_id_is_rejected(self) -> None:
        duplicate = dict(self.row)
        self._write_outcomes([self.row, duplicate])

        with self.assertRaisesRegex(CohortIntegrityError, "duplicate signal_id"):
            extract_clean_cohort(self.outcomes, expected_cohort_size=None)

    def test_clean_cohort_includes_otd_and_orr_but_excludes_other_families(self) -> None:
        rejection_reverse = {
            **self.row,
            "signal_id": "XAUUSD_SIGNAL_002",
            "scenario_family": "TPO_OPEN_REJECTION_REVERSE",
            "scenario_type": "SWEEP_RETURN_LONG",
            "direction": "LONG",
        }
        unrelated = {
            **self.row,
            "signal_id": "XAUUSD_SIGNAL_003",
            "scenario_family": "TPO_OPEN_AUCTION_BREAKOUT",
        }
        self._write_outcomes([self.row, rejection_reverse, unrelated])

        records = extract_clean_cohort(self.outcomes, expected_cohort_size=2)

        self.assertEqual(
            {record.scenario_family for record in records},
            {"TPO_OPEN_TEST_DRIVE", "TPO_OPEN_REJECTION_REVERSE"},
        )

    def test_normalization_deduplicates_and_sets_closed_bar_availability(self) -> None:
        duplicate_time = pd.Timestamp("2026-06-18T10:00:00+00:00")
        invalid_time = pd.Timestamp("2026-06-18T10:05:00+00:00")
        raw = pd.DataFrame(
            {
                "open": [100.0, 101.0, None],
                "high": [102.0, 103.0, 104.0],
                "low": [99.0, 100.0, 101.0],
                "close": [101.0, 102.0, 103.0],
            },
            index=[duplicate_time, duplicate_time, invalid_time],
        )

        normalized = normalize_ohlc_frame(
            raw,
            symbol="XAUUSD",
            timeframe="5m",
            source_provider="fixture",
        )

        self.assertEqual(normalized.duplicate_rows_discarded, 1)
        self.assertEqual(normalized.invalid_rows_discarded, 1)
        self.assertEqual(len(normalized.frame), 1)
        row = normalized.frame.iloc[0]
        self.assertEqual(row["open"], 101.0)
        self.assertEqual(row["bar_open_utc"], duplicate_time)
        self.assertEqual(row["bar_close_utc"], duplicate_time + pd.Timedelta(minutes=5))

    def test_replay_selection_is_closed_bar_as_of_and_stops_at_expiry(self) -> None:
        record = extract_clean_cohort(self.outcomes, expected_cohort_size=1)[0]
        raw_15m = pd.read_parquet(self.cache / "XAUUSD" / "15m.parquet")
        raw_5m = pd.read_parquet(self.five_minute_source / "XAUUSD" / "5m.parquet")
        bars_15m = normalize_ohlc_frame(
            raw_15m,
            symbol="XAUUSD",
            timeframe="15m",
            source_provider="fixture",
        ).frame
        bars_5m = normalize_ohlc_frame(
            raw_5m,
            symbol="XAUUSD",
            timeframe="5m",
            source_provider="fixture",
        ).frame

        selected_15m, selected_5m, coverage = select_replay_bars(
            [record],
            bars_15m,
            bars_5m,
            pre_event_bars=3,
        )

        decision = pd.Timestamp(record.decision_time_utc)
        expiry = pd.Timestamp(record.expires_at_utc)
        self.assertEqual(coverage[0]["coverage_5m"]["pre_event_closed_bars"], 3)
        self.assertEqual(coverage[0]["coverage_15m"]["pre_event_closed_bars"], 3)
        self.assertEqual(
            len(selected_5m.loc[selected_5m["bar_close_utc"] <= decision]),
            3,
        )
        self.assertEqual(
            len(selected_15m.loc[selected_15m["bar_close_utc"] <= decision]),
            3,
        )
        self.assertTrue((selected_5m["bar_close_utc"] <= expiry).all())
        self.assertTrue((selected_15m["bar_close_utc"] <= expiry).all())
        self.assertIn(
            pd.Timestamp("2026-06-18T10:00:00+00:00"),
            set(selected_5m["bar_open_utc"]),
        )
        self.assertNotIn(
            pd.Timestamp("2026-06-18T10:30:00+00:00"),
            set(selected_5m["bar_open_utc"]),
        )

    def test_builder_writes_only_new_versioned_artifact_with_full_audits(self) -> None:
        original_outcomes_hash = sha256_file(self.outcomes)
        original_15m_hash = sha256_file(self.cache / "XAUUSD" / "15m.parquet")
        original_5m_hash = sha256_file(
            self.five_minute_source / "XAUUSD" / "5m.parquet"
        )

        result = self._build()

        self.assertEqual(result["status"], "OK")
        self.assertEqual(original_outcomes_hash, sha256_file(self.outcomes))
        self.assertEqual(original_15m_hash, sha256_file(self.cache / "XAUUSD" / "15m.parquet"))
        self.assertEqual(
            original_5m_hash,
            sha256_file(self.five_minute_source / "XAUUSD" / "5m.parquet"),
        )
        self.assertEqual(
            {path.name for path in self.output.iterdir()},
            {
                "bars_15m.parquet",
                "bars_5m.parquet",
                "checksums.sha256",
                "cohort.json",
                "manifest.json",
            },
        )

        manifest = json.loads((self.output / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(
            manifest["version"],
            "otd-orr-ltf-backfill-v0.1.1-research-only",
        )
        self.assertTrue(manifest["research_only"])
        self.assertFalse(manifest["executable_signal"])
        self.assertEqual(manifest["battle_gate_impact"], "none")
        self.assertEqual(manifest["telegram_signal_impact"], "none")
        self.assertEqual(manifest["source_outcomes_sha256"], original_outcomes_hash)
        self.assertEqual(manifest["cohort"]["decision_time_source"], "signal_created_at_utc")
        self.assertEqual(
            manifest["cohort"]["selected_evaluation_audit"],
            {
                "within_original_lifecycle": 1,
                "at_or_after_original_expiry": 0,
                "context_use_rule": (
                    "selected evaluation metadata is audit-only and must not be "
                    "used before selected_evaluation_at_utc; an evaluation at or "
                    "after expiry is never replay-eligible context"
                ),
            },
        )
        self.assertTrue(manifest["coverage"][0]["coverage_complete"])
        self.assertEqual(manifest["audit_5m"]["source"], "local_versioned_input")
        self.assertEqual(manifest["audit_15m"]["source"], "production_parquet_cache")
        for audit_key in ("audit_5m", "audit_15m"):
            audit = manifest[audit_key]
            self.assertIn("normalized_source_sha256", audit)
            self.assertIn("source_duplicate_rows_discarded", audit)
            self.assertIn("selection_overlap_duplicate_rows_discarded", audit)
            self.assertIn("selected_rows_sha256", audit)
        for file_name, file_audit in manifest["files"].items():
            self.assertEqual(file_audit["sha256"], sha256_file(self.output / file_name))

        bars_5m = pd.read_parquet(self.output / "bars_5m.parquet")
        bars_15m = pd.read_parquet(self.output / "bars_15m.parquet")
        self.assertEqual(list(bars_5m.columns), BAR_COLUMNS)
        self.assertEqual(list(bars_15m.columns), BAR_COLUMNS)
        self.assertEqual(set(bars_5m["source_provider"]), {"local_versioned_input"})
        self.assertEqual(set(bars_15m["source_provider"]), {"production_parquet_cache"})

        manifest_text = json.dumps(manifest).lower()
        for forbidden in ("api_key", "apikey", "authorization", "password", "secret", "token"):
            self.assertNotIn(forbidden, manifest_text)

        checksum_lines = (self.output / "checksums.sha256").read_text(
            encoding="utf-8"
        ).splitlines()
        self.assertEqual(len(checksum_lines), 4)
        for line in checksum_lines:
            digest, file_name = line.split("  ", 1)
            self.assertEqual(digest, sha256_file(self.output / file_name))

    def test_existing_output_is_never_overwritten(self) -> None:
        self._build()
        before = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in self.output.iterdir()
        }

        with self.assertRaises(FileExistsError):
            self._build()

        after = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in self.output.iterdir()
        }
        self.assertEqual(before, after)

    def test_dry_run_creates_no_output(self) -> None:
        dry_output = self.root / "research" / "dry-run-dataset-v0.1"

        result = self._build(output=dry_output, dry_run=True)

        self.assertEqual(result["status"], "DRY_RUN_OK")
        self.assertFalse(result["output_created"])
        self.assertFalse(dry_output.exists())

    def test_missing_5m_coverage_fails_closed_without_output(self) -> None:
        short_path = self.five_minute_source / "XAUUSD" / "5m.parquet"
        self._write_ohlc(
            short_path,
            start="2026-06-18T10:00:00+00:00",
            periods=0,
            frequency="5min",
        )

        with self.assertRaisesRegex(CoverageError, "Incomplete closed-bar coverage"):
            self._build()

        self.assertFalse(self.output.exists())

    def test_live_fetch_requires_explicit_opt_in_and_does_not_touch_network(self) -> None:
        with patch(
            "scripts.build_otd_orr_ltf_backfill.fetch_live_5m"
        ) as fetch_mock:
            with self.assertRaisesRegex(BackfillError, "requires --allow-network-fetch"):
                build_dataset(
                    outcomes_path=self.outcomes,
                    cache_dir=self.cache,
                    output_dir=self.output,
                    five_minute_source="live",
                    expected_cohort_size=1,
                    pre_event_bars=3,
                )

        fetch_mock.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_provider_errors_never_echo_provider_exception_text(self) -> None:
        error = _safe_provider_error(
            "provider",
            "XAUUSD",
            RuntimeError("credential-value-must-never-escape"),
        )

        self.assertNotIn("credential-value", str(error))
        self.assertIn("RuntimeError", str(error))


if __name__ == "__main__":
    unittest.main()
