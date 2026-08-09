from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from app.services.research.canonical_universe_artifacts import (
    build_verified_zip,
    outcome_counts,
    semantic_identity,
    validate_outcome_counts,
)
from scripts.check_research_environment import check_environment
from scripts.run_canonical_max_depth_universe_v2 import (
    CANONICAL_SYMBOLS,
    DEVELOPMENT_THRESHOLDS,
    EVENT_COMPACT_COLUMNS,
    EXECUTION_COMPACT_COLUMNS,
    CanonicalRunnerError,
    _trade_path,
    build_parser,
    derive_holdout_cutoff,
    flatten_event,
    main,
    validate_args,
)

BASE = [
    "--output-root",
    "/tmp/out",
    "--cache-root",
    "/tmp/cache",
    "--start-date",
    "2026-01-01",
    "--end-date",
    "2026-01-02",
    "--data-cutoff-utc",
    "2026-01-03T00:00:00Z",
]


class CanonicalUniverseRunnerTests(unittest.TestCase):
    def test_cli_has_separate_data_and_holdout_cutoffs(self) -> None:
        args = build_parser().parse_args(
            BASE + ["--holdout-cutoff-utc", "2026-01-02T00:00:00Z", "--plan-only"]
        )
        self.assertEqual(args.data_cutoff_utc, "2026-01-03T00:00:00Z")
        self.assertEqual(args.holdout_cutoff_utc, "2026-01-02T00:00:00Z")

    def test_modes_are_mutually_exclusive(self) -> None:
        with self.assertRaises(SystemExit):
            build_parser().parse_args(BASE + ["--plan-only", "--verify-only"])

    def test_invalid_mode_combinations_fail_before_work(self) -> None:
        args = build_parser().parse_args(
            BASE + ["--verify-only", "--allow-network-fetch"]
        )
        with self.assertRaisesRegex(CanonicalRunnerError, "offline modes"):
            validate_args(args)
        args = build_parser().parse_args(BASE + ["--plan-only", "--build-zip"])
        with self.assertRaisesRegex(CanonicalRunnerError, "build-zip"):
            validate_args(args)

    def test_full_canonical_universe_is_exactly_thirteen_symbols(self) -> None:
        self.assertEqual(len(CANONICAL_SYMBOLS), 13)
        self.assertEqual(len(set(CANONICAL_SYMBOLS)), 13)

    def test_holdout_cutoff_is_derived_once_from_full_event_times(self) -> None:
        events = [
            {"confirmed_at_utc": f"2026-01-0{day}T00:00:00Z"} for day in range(1, 5)
        ]
        authoritative = {
            "holdout_start_utc": "2026-01-03T00:00:00+00:00",
            "holdout_status": "AVAILABLE",
        }
        with patch(
            "scripts.run_canonical_max_depth_universe_v2.compile_event_census",
            return_value=authoritative,
        ) as compiler:
            cutoff, census = derive_holdout_cutoff(events, [], 0.5)
        self.assertEqual(cutoff, datetime(2026, 1, 3, tzinfo=UTC))
        self.assertIs(census, authoritative)
        self.assertEqual(compiler.call_args.kwargs["event_records"], events)

    def test_event_ablation_thresholds_and_minutes_are_flattened(self) -> None:
        hits = {f"{value:g}R": None for value in DEVELOPMENT_THRESHOLDS}
        hits["0.5R"] = "2026-01-01T00:10:00Z"
        row = flatten_event(
            {
                "candidate_id": "event-1",
                "confirmed_at_utc": "2026-01-01T00:00:00Z",
                "threshold_hits_utc": hits,
                "event_mfe_R": 0.75,
                "observation_end_utc": "2026-01-01T00:30:00Z",
            }
        )
        self.assertEqual(row["minutes_confirmation_to_0.50R"], 10.0)
        self.assertIn("minutes_confirmation_to_3.00R", EVENT_COMPACT_COLUMNS)
        self.assertIn("remaining_target_fraction_at_fill", EXECUTION_COMPACT_COLUMNS)

    def test_environment_contract_rejects_python_313(self) -> None:
        with (
            patch("scripts.check_research_environment.sys.version_info", (3, 13, 1)),
            self.assertRaisesRegex(RuntimeError, "Python 3.12.x"),
        ):
            check_environment(Path("/unused-output"), Path("/unused-cache"))

    def test_explicit_outcome_fields_enforce_arithmetic(self) -> None:
        value = outcome_counts(
            [{"outcome": "TP_HIT"}, {"outcome": "SL_HIT"}, {"outcome": "OPEN"}]
        )
        self.assertEqual(
            value, {"tp_count": 1, "sl_count": 1, "resolved_count": 2, "win_rate": 0.5}
        )
        with self.assertRaisesRegex(ValueError, "resolved_count"):
            validate_outcome_counts(
                {"tp_count": 1, "sl_count": 1, "resolved_count": 3, "win_rate": 1 / 3}
            )

    def test_semantic_and_ordered_id_hashes_are_repeatable(self) -> None:
        frame = pd.DataFrame(
            {
                "candidate_id": ["a", "b"],
                "confirmed_at_utc": ["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"],
                "value": [1, 2],
            }
        )
        kwargs = {
            "id_column": "candidate_id",
            "timestamp_column": "confirmed_at_utc",
            "code_revision": "abc",
            "environment_versions": {"python": "3.12.3"},
            "source_hashes": ["def"],
            "data_cutoff_utc": "2026-01-03T00:00:00+00:00",
            "holdout_cutoff_utc": "2026-01-02T00:00:00+00:00",
        }
        self.assertEqual(
            semantic_identity(frame, **kwargs),
            semantic_identity(frame.iloc[::-1].copy(), **kwargs),
        )

    def test_diagnostic_subset_cannot_use_canonical_zip_name(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "artifact.json"
            path.write_text("{}\n")
            with self.assertRaisesRegex(ValueError, "does not prove"):
                build_verified_zip(Path(root), [path], frozen_manifest={})

    def test_semantic_identity_rejects_missing_null_and_duplicate_ids(self) -> None:
        frame = pd.DataFrame(
            {"candidate_id": ["a"], "confirmed_at_utc": ["2026-01-01T00:00:00Z"]}
        )
        kwargs = {
            "id_column": "candidate_id",
            "timestamp_column": "confirmed_at_utc",
            "code_revision": "abc",
            "environment_versions": {},
            "source_hashes": [],
            "data_cutoff_utc": "cutoff",
            "holdout_cutoff_utc": "holdout",
        }
        with self.assertRaisesRegex(ValueError, "required"):
            semantic_identity(frame.drop(columns="candidate_id"), **kwargs)
        with self.assertRaisesRegex(ValueError, "non-null"):
            semantic_identity(
                pd.concat([frame, frame.assign(candidate_id=None)]), **kwargs
            )
        with self.assertRaisesRegex(ValueError, "unique"):
            semantic_identity(pd.concat([frame, frame]), **kwargs)

    def test_trade_path_starts_after_fill_and_preserves_post_stop_path(self) -> None:
        history = pd.DataFrame(
            [
                {"bar_close_utc": "2026-01-01T00:05:00Z", "high": 103.0, "low": 100.0},
                {"bar_close_utc": "2026-01-01T00:10:00Z", "high": 101.0, "low": 98.5},
                {"bar_close_utc": "2026-01-01T00:15:00Z", "high": 103.0, "low": 99.5},
            ]
        )
        row = {
            "filled_at_utc": "2026-01-01T00:05:00Z",
            "trade_resolution_expires_at_utc": "2026-01-01T00:15:00Z",
            "entry_reference_price": 100.0,
            "invalidation_reference_price": 99.0,
            "target_reference_price": 102.0,
            "outcome": "SL_HIT",
        }
        result = _trade_path(row, history, "LONG")
        self.assertEqual(
            result["trade_first_1R_hit_at_utc"], "2026-01-01T00:15:00+00:00"
        )
        self.assertEqual(result["trade_stop_hit_at_utc"], "2026-01-01T00:10:00+00:00")
        self.assertEqual(
            result["trade_real_target_hit_at_utc"], "2026-01-01T00:15:00+00:00"
        )
        self.assertEqual(row["outcome"], "SL_HIT")

    def test_trade_same_bar_stop_and_target_is_conservatively_ambiguous(self) -> None:
        history = pd.DataFrame(
            [{"bar_close_utc": "2026-01-01T00:10:00Z", "high": 102.5, "low": 98.5}]
        )
        result = _trade_path(
            {
                "filled_at_utc": "2026-01-01T00:05:00Z",
                "trade_resolution_expires_at_utc": "2026-01-01T00:10:00Z",
                "entry_reference_price": 100.0,
                "invalidation_reference_price": 99.0,
                "target_reference_price": 102.0,
            },
            history,
            "LONG",
        )
        self.assertTrue(result["trade_same_bar_ambiguity"])
        self.assertEqual(
            result["trade_stop_hit_at_utc"], result["trade_real_target_hit_at_utc"]
        )

    def test_offline_replay_then_canonical_verify_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            output = root_path / "output"
            cache = root_path / "cache"
            history_path = cache / "history" / "EURUSD_5m.parquet"
            history_path.parent.mkdir(parents=True)
            pd.DataFrame(
                [
                    {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "open": 1.0,
                        "high": 1.1,
                        "low": 0.9,
                        "close": 1.0,
                        "volume": 1.0,
                    }
                ]
            ).to_parquet(history_path, index=False)
            candidates = [
                SimpleNamespace(
                    candidate_id=f"event-{index}",
                    activated_at_utc=datetime(
                        2026, 1, 1, 0, 5 + index * 10, tzinfo=UTC
                    ),
                    payload={"event_census_execution_eligible": True},
                )
                for index in range(2)
            ]
            events = [
                {
                    "candidate_id": candidate.candidate_id,
                    "symbol": "EURUSD",
                    "setup_family": "OPEN_TEST_DRIVE",
                    "direction": "LONG",
                    "session_id": "s",
                    "session_open_utc": "2026-01-01T00:00:00Z",
                    "confirmed_at_utc": candidate.activated_at_utc.isoformat(),
                    "event_outcome": "DEVELOPED",
                    "event_evaluable": True,
                }
                for candidate in candidates
            ]
            executions = [
                {
                    "candidate_id": candidate.candidate_id,
                    "symbol": "EURUSD",
                    "setup_family": "OPEN_TEST_DRIVE",
                    "direction": "LONG",
                    "session_id": "s",
                    "activated_at_utc": candidate.activated_at_utc.isoformat(),
                    "ready": True,
                    "filled_at_utc": candidate.activated_at_utc.isoformat(),
                    "outcome": "TP_HIT" if index == 0 else "SL_HIT",
                }
                for index, candidate in enumerate(candidates)
            ]
            base = [
                "--output-root",
                str(output),
                "--cache-root",
                str(cache),
                "--symbols",
                "EURUSD",
                "--start-date",
                "2026-01-01",
                "--end-date",
                "2026-01-01",
                "--data-cutoff-utc",
                "2026-01-02T00:00:00Z",
                "--holdout-cutoff-utc",
                "2026-01-01T00:10:00Z",
            ]
            audit = {"cache_complete": True, "partitions": [{"source_sha256": "abc"}]}
            with (
                patch(
                    "scripts.run_canonical_max_depth_universe_v2.check_environment",
                    return_value={"versions": {"python": "3.12"}},
                ),
                patch(
                    "scripts.run_canonical_max_depth_universe_v2.DukascopyMaxDepthProvider.verify_cache",
                    return_value=audit,
                ),
                patch(
                    "scripts.run_canonical_max_depth_universe_v2.reconstruct_tpo_watch_candidates",
                    return_value=(candidates, {}),
                ),
                patch(
                    "scripts.run_canonical_max_depth_universe_v2.enrich_event",
                    side_effect=events,
                ),
                patch(
                    "scripts.run_canonical_max_depth_universe_v2.enrich_execution",
                    side_effect=executions,
                ),
                patch(
                    "scripts.run_canonical_max_depth_universe_v2.measure_event_development",
                    return_value={},
                ),
                patch(
                    "scripts.run_canonical_max_depth_universe_v2.replay_candidate",
                    return_value={},
                ),
            ):
                self.assertEqual(main(base + ["--replay-only"]), 0)
            self.assertTrue((output / "otd_orr_event_census_v2.parquet").is_file())
            self.assertTrue(
                (output / "execution_candidates_v2_compact.csv.gz").is_file()
            )
            with patch(
                "scripts.run_canonical_max_depth_universe_v2.check_environment",
                return_value={"versions": {"python": "3.12"}},
            ):
                self.assertEqual(main(base + ["--verify-only"]), 0)
            self.assertFalse(
                json.loads((output / "frozen_universe_v2_manifest.json").read_text())[
                    "universe_frozen"
                ]
            )


if __name__ == "__main__":
    unittest.main()
