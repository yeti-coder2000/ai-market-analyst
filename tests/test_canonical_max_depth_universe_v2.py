from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
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
    build_parser,
    derive_holdout_cutoff,
    flatten_event,
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
        self.assertEqual(
            derive_holdout_cutoff(events, 0.5), datetime(2026, 1, 3, tzinfo=UTC)
        )

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
            semantic_identity(frame.copy(), **kwargs),
        )

    def test_diagnostic_subset_cannot_use_canonical_zip_name(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "artifact.json"
            path.write_text("{}\n")
            with self.assertRaisesRegex(ValueError, "reserved"):
                build_verified_zip(Path(root), [path], universe_frozen=False)


if __name__ == "__main__":
    unittest.main()
