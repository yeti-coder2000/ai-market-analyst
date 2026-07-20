from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.services.signal_outcome_tracker import (
    RESEARCH_INTEGRITY_V2_OUTPUT_SCHEMA,
    build_research_alert_from_battle_event,
    is_research_battle_event,
    load_research_counterfactual_alerts,
    track_outcomes,
)


def battle_event(**overrides: object) -> dict[str, object]:
    event: dict[str, object] = {
        "event_type": "battle_permission_evaluated",
        "ts_utc": "2026-07-01T10:00:00+00:00",
        "signal_id": "BTCUSD_SIGNAL_1",
        "symbol": "BTCUSD",
        "direction": "LONG",
        "entry_reference_price": 100.0,
        "invalidation_reference_price": 95.0,
        "target_reference_price": 110.0,
        "risk_reward_ratio": 2.0,
        "practical_rr": 2.0,
        "battle_permission": "RESEARCH_ONLY",
        "telegram_delivery_mode": "RESEARCH_ALERT",
        "sent_to_telegram": False,
        "execution_status": "EXECUTABLE",
        "entry_timing_status": "ENTRY_ACTIONABLE",
        "signal_created_at_utc": "2026-07-01T10:00:00+00:00",
        "signal_age_minutes": 0.0,
        "signal_max_age_minutes": 60.0,
        "signal_freshness_status": "FRESH",
        "scenario_type": "TPO_OPEN_TEST_DRIVE_LONG",
    }
    event.update(overrides)
    return event


def write_ndjson(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


class ResearchCounterfactualIntegrityTest(unittest.TestCase):
    def test_v2_includes_research_only_and_excludes_not_ready_or_non_actionable(self) -> None:
        eligible = battle_event()
        fresh_retest = battle_event(
            signal_id="BTCUSD_SIGNAL_2",
            entry_timing_status="ENTRY_ACTIONABLE_AFTER_FRESH_RETEST",
        )
        not_ready = battle_event(
            signal_id="BTCUSD_SIGNAL_3",
            battle_permission="NOT_READY",
        )
        late = battle_event(
            signal_id="BTCUSD_SIGNAL_4",
            entry_timing_status="LATE_SIGNAL",
        )

        self.assertTrue(is_research_battle_event(eligible, integrity_v2=True))
        self.assertTrue(is_research_battle_event(fresh_retest, integrity_v2=True))
        self.assertFalse(is_research_battle_event(not_ready, integrity_v2=True))
        self.assertFalse(is_research_battle_event(late, integrity_v2=True))

        # The scheduled legacy path remains unchanged until a separate approval.
        self.assertFalse(is_research_battle_event(eligible, integrity_v2=False))

        legacy_alert = build_research_alert_from_battle_event(
            battle_event(battle_permission="BLOCKED_BY_CONTEXT"),
            integrity_v2=False,
        )
        self.assertIsNotNone(legacy_alert)
        assert legacy_alert is not None
        self.assertEqual(legacy_alert["schema_version"], "research-counterfactual-v1")
        self.assertNotIn("expires_at_utc", legacy_alert)
        self.assertNotIn("selection_policy", legacy_alert)
        self.assertNotIn("entry_timing_status", legacy_alert)

    def test_v2_deduplicates_by_signal_id_using_first_eligible_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            events_path = Path(tmp) / "battle_permission_events.ndjson"
            rows = [
                battle_event(
                    ts_utc="2026-07-01T10:08:00+00:00",
                    battle_permission="BLOCKED_BY_CONTEXT",
                    entry_reference_price=99.0,
                ),
                battle_event(
                    ts_utc="2026-07-01T10:00:00+00:00",
                    battle_permission="NOT_READY",
                    entry_reference_price=103.0,
                ),
                battle_event(
                    ts_utc="2026-07-01T10:07:00+00:00",
                    battle_permission="RESEARCH_ONLY",
                    entry_reference_price=100.0,
                ),
                battle_event(
                    ts_utc="2026-07-01T10:05:00+00:00",
                    entry_timing_status="LATE_SIGNAL",
                    entry_reference_price=102.0,
                ),
                battle_event(
                    ts_utc="2026-07-01T10:06:00+00:00",
                    target_reference_price=None,
                    entry_reference_price=101.0,
                ),
                battle_event(
                    signal_id="ETHUSD_SIGNAL_1",
                    symbol="ETHUSD",
                    ts_utc="2026-07-01T10:06:00+00:00",
                    battle_permission="BLOCKED_BY_CONTEXT",
                    entry_reference_price=200.0,
                    invalidation_reference_price=190.0,
                    target_reference_price=220.0,
                ),
            ]
            write_ndjson(events_path, rows)

            selection_audit: dict[str, int] = {}
            alerts = load_research_counterfactual_alerts(
                events_path,
                integrity_v2=True,
                selection_audit=selection_audit,
            )

        self.assertEqual(len(alerts), 2)
        by_signal = {str(alert["signal_id"]): alert for alert in alerts}
        selected = by_signal["BTCUSD_SIGNAL_1"]
        self.assertEqual(selected["source_event_ts_utc"], "2026-07-01T10:07:00+00:00")
        self.assertEqual(selected["entry_reference_price"], 100.0)
        self.assertEqual(selected["battle_permission"], "RESEARCH_ONLY")
        self.assertEqual(selected["alert_id"], "RESEARCH_BTCUSD_SIGNAL_1")
        self.assertEqual(selection_audit["raw_events"], 6)
        self.assertEqual(selection_audit["eligible_evaluations"], 4)
        self.assertEqual(selection_audit["invalid_execution_plans"], 1)
        self.assertEqual(selection_audit["duplicate_eligible_evaluations_discarded"], 1)
        self.assertEqual(selection_audit["selected_unique_signals"], 2)

    def test_expiry_uses_original_signal_lifecycle(self) -> None:
        alert = build_research_alert_from_battle_event(
            battle_event(
                ts_utc="2026-07-01T10:45:00+00:00",
                signal_created_at_utc="2026-07-01T10:00:00+00:00",
                signal_age_minutes=45.0,
                signal_max_age_minutes=60.0,
            ),
            integrity_v2=True,
        )

        self.assertIsNotNone(alert)
        assert alert is not None
        self.assertEqual(alert["expires_at_utc"], "2026-07-01T11:00:00+00:00")
        self.assertEqual(
            alert["expiry_source"],
            "signal_created_at_utc_plus_signal_max_age_minutes",
        )

    def test_snapshot_after_expiry_cannot_trigger_late_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            alerts_path = root / "telegram_alerts.json"
            snapshots_path = root / "radar_snapshot_v2.ndjson"
            outcomes_path = root / "signal_outcomes.json"
            events_path = root / "battle_permission_events.ndjson"
            dry_run_output = root / "signal_outcomes.research_integrity_v2.json"

            alerts_path.write_text("[]", encoding="utf-8")
            outcomes_path.write_text('{"legacy": true}', encoding="utf-8")
            write_ndjson(events_path, [battle_event()])
            write_ndjson(
                snapshots_path,
                [
                    {
                        "symbol": "BTCUSD",
                        "ts_utc": "2026-07-01T12:00:00+00:00",
                        "open": 101.0,
                        "high": 111.0,
                        "low": 94.0,
                        "close": 100.0,
                    }
                ],
            )

            raw_inputs_before = {
                path: path.read_bytes()
                for path in (alerts_path, snapshots_path, events_path, outcomes_path)
            }

            result = track_outcomes(
                alerts_path=alerts_path,
                snapshot_path=snapshots_path,
                outcomes_path=outcomes_path,
                battle_events_path=events_path,
                dry_run=True,
                research_integrity_v2=True,
                dry_run_output_path=dry_run_output,
            )

            payload = json.loads(dry_run_output.read_text(encoding="utf-8"))
            record = payload["signals"][0]

            self.assertTrue(result["research_integrity_v2"])
            self.assertEqual(payload["schema_version"], RESEARCH_INTEGRITY_V2_OUTPUT_SCHEMA)
            self.assertEqual(record["outcome_status"], "EXPIRED")
            self.assertFalse(record["entry_triggered"])
            self.assertEqual(record["expired_at_utc"], "2026-07-01T11:00:00+00:00")

            for path, expected_bytes in raw_inputs_before.items():
                self.assertEqual(path.read_bytes(), expected_bytes)

    def test_snapshot_inside_expiry_can_resolve_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            alerts_path = root / "telegram_alerts.json"
            snapshots_path = root / "radar_snapshot_v2.ndjson"
            outcomes_path = root / "signal_outcomes.json"
            events_path = root / "battle_permission_events.ndjson"
            dry_run_output = root / "signal_outcomes.research_integrity_v2.json"

            alerts_path.write_text("[]", encoding="utf-8")
            outcomes_path.write_text('{"legacy": true}', encoding="utf-8")
            write_ndjson(
                events_path,
                [battle_event(signal_max_age_minutes=360.0)],
            )
            write_ndjson(
                snapshots_path,
                [
                    {
                        "symbol": "BTCUSD",
                        "ts_utc": "2026-07-01T10:15:00+00:00",
                        "open": 101.0,
                        "high": 102.0,
                        "low": 99.0,
                        "close": 101.0,
                    },
                    {
                        "symbol": "BTCUSD",
                        "ts_utc": "2026-07-01T10:30:00+00:00",
                        "open": 101.0,
                        "high": 111.0,
                        "low": 100.0,
                        "close": 110.0,
                    },
                ],
            )

            track_outcomes(
                alerts_path=alerts_path,
                snapshot_path=snapshots_path,
                outcomes_path=outcomes_path,
                battle_events_path=events_path,
                dry_run=True,
                research_integrity_v2=True,
                dry_run_output_path=dry_run_output,
            )

            payload = json.loads(dry_run_output.read_text(encoding="utf-8"))
            record = payload["signals"][0]
            self.assertEqual(record["outcome_status"], "TP_HIT")
            self.assertTrue(record["entry_triggered"])
            self.assertEqual(record["entry_triggered_at_utc"], "2026-07-01T10:15:00+00:00")

    def test_versioned_output_refuses_overwrite_and_protected_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            alerts_path = root / "telegram_alerts.json"
            snapshots_path = root / "radar_snapshot_v2.ndjson"
            outcomes_path = root / "signal_outcomes.json"
            events_path = root / "battle_permission_events.ndjson"
            existing_output = root / "existing.integrity.json"

            alerts_path.write_text("[]", encoding="utf-8")
            snapshots_path.write_text("", encoding="utf-8")
            outcomes_path.write_text('{"legacy": true}', encoding="utf-8")
            write_ndjson(events_path, [battle_event()])
            existing_output.write_text('{"keep": true}', encoding="utf-8")

            legacy_result = track_outcomes(
                alerts_path=alerts_path,
                snapshot_path=snapshots_path,
                outcomes_path=outcomes_path,
                battle_events_path=events_path,
                dry_run=True,
            )
            self.assertNotIn("research_integrity_v2", legacy_result)
            self.assertEqual(legacy_result["research_counterfactual_alerts"], 0)
            self.assertEqual(outcomes_path.read_text(encoding="utf-8"), '{"legacy": true}')

            with self.assertRaises(FileExistsError):
                track_outcomes(
                    alerts_path=alerts_path,
                    snapshot_path=snapshots_path,
                    outcomes_path=outcomes_path,
                    battle_events_path=events_path,
                    dry_run=True,
                    research_integrity_v2=True,
                    dry_run_output_path=existing_output,
                )

            self.assertEqual(existing_output.read_text(encoding="utf-8"), '{"keep": true}')

            with self.assertRaises(ValueError):
                track_outcomes(
                    alerts_path=alerts_path,
                    snapshot_path=snapshots_path,
                    outcomes_path=outcomes_path,
                    battle_events_path=events_path,
                    dry_run=True,
                    research_integrity_v2=True,
                    dry_run_output_path=outcomes_path,
                )

            self.assertEqual(outcomes_path.read_text(encoding="utf-8"), '{"legacy": true}')


if __name__ == "__main__":
    unittest.main()
