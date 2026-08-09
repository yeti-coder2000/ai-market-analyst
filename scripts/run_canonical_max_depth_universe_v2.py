from __future__ import annotations

"""Research-only canonical max-depth universe v2 orchestration.

This entry point is intentionally separate from the Twelve Data/Yahoo runner.
The implemented provider profile is tick-aggregated and explicitly non-parity;
network acquisition is blocked pending proof of native SWFX/CFD bid M5 format
and the complete 13-instrument mapping.
"""

import argparse
import math
import shlex
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.ltf_execution_backtest import (
    HistoricalWatchCandidate,
    normalize_m5_history,
    reconstruct_tpo_watch_candidates,
    replay_candidate,
)
from app.services.otd_orr_event_census import measure_event_development
from app.services.research.canonical_universe_artifacts import (
    code_sha,
    outcome_counts,
    semantic_identity,
    sha256_file,
    write_compact_csv_gz,
    write_json,
)
from app.services.research.dukascopy_max_depth_provider import (
    PROFILE_NAME,
    DukascopyMaxDepthProvider,
)
from app.services.research.historical_m5_provider import HistoricalM5Request
from scripts.check_research_environment import check_environment

VERSION = "canonical-max-depth-universe-v2.0.0"
CANONICAL_SYMBOLS = (
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "AUDUSD",
    "BTCUSD",
    "ETHUSD",
    "GER40",
    "NAS100",
    "SPX500",
    "UKOIL",
)
DEVELOPMENT_THRESHOLDS = (0.50, 0.75, 1.00, 1.25, 1.50, 2.00, 2.50, 3.00)
HISTORICAL_REFERENCE = {
    "m5_bar_count": 20_442_071,
    "event_count": 13_846,
    "candidate_count": 3_093,
    "ready_count": 115,
    "fill_count": 44,
    "full": {"tp_count": 6, "sl_count": 26, "resolved_count": 32, "win_rate": 0.1875},
    "development": {
        "tp_count": 3,
        "sl_count": 11,
        "resolved_count": 14,
        "win_rate": 3 / 14,
    },
    "holdout": {
        "tp_count": 3,
        "sl_count": 15,
        "resolved_count": 18,
        "win_rate": 3 / 18,
    },
}

EVENT_COMPACT_COLUMNS = (
    "candidate_id",
    "symbol",
    "setup_family",
    "direction",
    "session_id",
    "session_open_utc",
    "confirmed_at_utc",
    "holdout_cohort",
    "max_development_R",
    "max_development_at_utc",
    "event_outcome",
    "event_evaluable",
    *(f"hit_{value:.2f}R_at_utc" for value in DEVELOPMENT_THRESHOLDS),
    *(f"minutes_confirmation_to_{value:.2f}R" for value in DEVELOPMENT_THRESHOLDS),
)
EXECUTION_COMPACT_COLUMNS = (
    "candidate_id",
    "symbol",
    "setup_family",
    "direction",
    "session_id",
    "activated_at_utc",
    "holdout_cohort",
    "ready",
    "ready_at_utc",
    "filled_at_utc",
    "outcome",
    "event_progress_R_at_ready",
    "event_progress_R_at_fill",
    "max_event_R_before_ready",
    "max_event_R_before_fill",
    "remaining_target_R_at_ready",
    "remaining_target_R_at_fill",
    "remaining_target_fraction_at_ready",
    "remaining_target_fraction_at_fill",
    "first_0.50R_hit_at_utc",
    "first_1.00R_hit_at_utc",
    "first_1.50R_hit_at_utc",
    "first_2.00R_hit_at_utc",
    "first_2.50R_hit_at_utc",
    "first_3.00R_hit_at_utc",
    "stop_hit_at_utc",
    "real_target_hit_at_utc",
    "MFE_R",
    "MAE_R",
    "same_bar_ambiguity",
)


class CanonicalRunnerError(RuntimeError):
    pass


def utc(value: Any) -> datetime:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise CanonicalRunnerError("invalid UTC timestamp")
    stamp = (
        stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
    )
    return stamp.to_pydatetime()


def derive_holdout_cutoff(
    events: Sequence[Mapping[str, Any]], fraction: float
) -> datetime:
    times = sorted({utc(row["confirmed_at_utc"]) for row in events})
    if len(times) < 2:
        raise CanonicalRunnerError(
            "cannot derive holdout cutoff without two distinct event times"
        )
    index = max(1, min(len(times) - 1, round(len(times) * (1.0 - fraction))))
    return times[index]


def _threshold_key(value: float) -> str:
    return f"{value:g}R"


def flatten_event(record: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(record)
    hits = row.get("threshold_hits_utc") or {}
    confirmation = utc(row["confirmed_at_utc"])
    max_value, max_at = 0.0, None
    for value in DEVELOPMENT_THRESHOLDS:
        hit = hits.get(_threshold_key(value))
        row[f"hit_{value:.2f}R_at_utc"] = hit
        row[f"minutes_confirmation_to_{value:.2f}R"] = (
            round((utc(hit) - confirmation).total_seconds() / 60.0, 4) if hit else None
        )
        if hit:
            max_value, max_at = value, hit
    measured = row.get("event_mfe_R")
    if measured is not None and float(measured) >= max_value:
        max_value = float(measured)
        max_at = row.get("observation_end_utc")
    row["max_development_R"] = max_value
    row["max_development_at_utc"] = max_at
    return row


def enrich_event(
    candidate: HistoricalWatchCandidate,
    record: Mapping[str, Any],
    history: pd.DataFrame,
) -> dict[str, Any]:
    """Add ablation fields without changing census outcome classification."""
    row = flatten_event(record)
    end = row.get("observation_end_utc") or candidate.expires_at_utc
    terminal_reason = str(row.get("terminal_reason") or "")
    if terminal_reason not in {
        "SESSION_HORIZON_EXPIRED",
        "RIGHT_CENSORED_BEFORE_SESSION_HORIZON",
    }:
        end = utc(end) - pd.Timedelta(minutes=5)
    progress, at = _progress(candidate, history, end)
    if progress is not None:
        row["max_development_R"] = progress
        row["max_development_at_utc"] = at
    return row


def _progress(
    candidate: HistoricalWatchCandidate, history: pd.DataFrame, at: Any
) -> tuple[float | None, str | None]:
    if not at:
        return None, None
    closes = pd.to_datetime(history["bar_close_utc"], utc=True)
    rows = history.loc[
        (closes > pd.Timestamp(candidate.activated_at_utc))
        & (closes <= pd.Timestamp(utc(at)))
    ]
    activation = history.loc[closes == pd.Timestamp(candidate.activated_at_utc)]
    if activation.empty or rows.empty:
        return 0.0, None
    reference = float(activation.iloc[-1]["close"])
    risk = abs(reference - candidate.test_extreme)
    if risk <= 0:
        return None, None
    if candidate.direction == "LONG":
        excursions = (rows["high"] - reference) / risk
    else:
        excursions = (reference - rows["low"]) / risk
    index = excursions.astype(float).idxmax()
    return max(0.0, float(excursions.loc[index])), utc(
        rows.loc[index, "bar_close_utc"]
    ).isoformat()


def _first_hit(
    candidate: HistoricalWatchCandidate,
    history: pd.DataFrame,
    at: Any,
    threshold: float,
) -> str | None:
    if not at:
        return None
    closes = pd.to_datetime(history["bar_close_utc"], utc=True)
    activation = history.loc[closes == pd.Timestamp(candidate.activated_at_utc)]
    if activation.empty:
        return None
    reference = float(activation.iloc[-1]["close"])
    risk = abs(reference - candidate.test_extreme)
    if risk <= 0:
        return None
    rows = history.loc[
        (closes > pd.Timestamp(candidate.activated_at_utc))
        & (closes <= pd.Timestamp(utc(at)))
    ]
    favorable = (
        (rows["high"] - reference) / risk
        if candidate.direction == "LONG"
        else (reference - rows["low"]) / risk
    )
    hits = rows.loc[favorable >= threshold]
    return utc(hits.iloc[0]["bar_close_utc"]).isoformat() if not hits.empty else None


def enrich_execution(
    candidate: HistoricalWatchCandidate, row: Mapping[str, Any], history: pd.DataFrame
) -> dict[str, Any]:
    result = dict(row)
    ready_progress, _ = _progress(candidate, history, result.get("ready_at_utc"))
    fill_progress, _ = _progress(candidate, history, result.get("filled_at_utc"))
    target_r = result.get("risk_reward_ratio")
    target_r = float(target_r) if target_r is not None else None
    result.update(
        {
            "event_progress_R_at_ready": ready_progress,
            "event_progress_R_at_fill": fill_progress,
            "max_event_R_before_ready": ready_progress,
            "max_event_R_before_fill": fill_progress,
            "remaining_target_R_at_ready": max(0.0, target_r - ready_progress)
            if target_r is not None and ready_progress is not None
            else None,
            "remaining_target_R_at_fill": max(0.0, target_r - fill_progress)
            if target_r is not None and fill_progress is not None
            else None,
            "remaining_target_fraction_at_ready": max(0.0, target_r - ready_progress)
            / target_r
            if target_r and ready_progress is not None
            else None,
            "remaining_target_fraction_at_fill": max(0.0, target_r - fill_progress)
            / target_r
            if target_r and fill_progress is not None
            else None,
            "stop_hit_at_utc": result.get("resolved_at_utc")
            if result.get("outcome") == "SL_HIT"
            else None,
            "real_target_hit_at_utc": result.get("resolved_at_utc")
            if result.get("outcome") == "TP_HIT"
            else None,
            "MFE_R": result.get("mfe_R"),
            "MAE_R": result.get("mae_R"),
            "same_bar_ambiguity": "AMBIGUOUS" in str(result.get("outcome") or ""),
        }
    )
    for value in (0.5, 1.0, 1.5, 2.0, 2.5, 3.0):
        result[f"first_{value:.2f}R_hit_at_utc"] = _first_hit(
            candidate,
            history,
            result.get("resolved_at_utc") or candidate.expires_at_utc,
            value,
        )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the research-only canonical max-depth universe v2"
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--provider-profile", default=PROFILE_NAME)
    parser.add_argument("--symbols", default=",".join(CANONICAL_SYMBOLS))
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--data-cutoff-utc", required=True)
    parser.add_argument("--holdout-cutoff-utc")
    parser.add_argument("--holdout-fraction", type=float, default=0.30)
    parser.add_argument("--primary-development-r", type=float, default=1.5)
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--download-only", action="store_true")
    modes.add_argument("--replay-only", action="store_true")
    modes.add_argument("--plan-only", action="store_true")
    modes.add_argument("--verify-only", action="store_true")
    parser.add_argument("--allow-network-fetch", action="store_true")
    parser.add_argument("--build-zip", action="store_true")
    return parser


def validate_args(
    args: argparse.Namespace,
) -> tuple[list[str], datetime, datetime, datetime]:
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    if (
        not symbols
        or len(symbols) != len(set(symbols))
        or any(symbol not in CANONICAL_SYMBOLS for symbol in symbols)
    ):
        raise CanonicalRunnerError("symbols must be unique canonical approved symbols")
    if (
        not 0.10 <= args.holdout_fraction <= 0.50
        or not math.isfinite(args.primary_development_r)
        or args.primary_development_r <= 0
    ):
        raise CanonicalRunnerError("invalid holdout fraction or development R")
    if not 1 <= args.max_workers <= 16:
        raise CanonicalRunnerError("max-workers must be between 1 and 16")
    if args.provider_profile != PROFILE_NAME:
        raise CanonicalRunnerError(
            "only the explicit non-parity diagnostic profile is implemented"
        )
    if args.build_zip and (args.download_only or args.plan_only or args.verify_only):
        raise CanonicalRunnerError("build-zip requires full or replay-only mode")
    if args.resume and (args.replay_only or args.plan_only or args.verify_only):
        raise CanonicalRunnerError("resume is only valid for acquisition modes")
    if args.allow_network_fetch and (
        args.replay_only or args.plan_only or args.verify_only
    ):
        raise CanonicalRunnerError("offline modes cannot authorize network access")
    start = datetime.combine(pd.Timestamp(args.start_date).date(), time.min, tzinfo=UTC)
    end = datetime.combine(
        pd.Timestamp(args.end_date).date(), time.max, tzinfo=UTC
    ).replace(microsecond=0)
    cutoff = utc(args.data_cutoff_utc)
    if start > end or end > cutoff:
        raise CanonicalRunnerError("start/end/data cutoff ordering is invalid")
    if cutoff.minute % 5 or cutoff.second or cutoff.microsecond:
        raise CanonicalRunnerError("data cutoff must be a fully closed M5 boundary")
    return symbols, start, end, cutoff


def _invocation(argv: Sequence[str] | None) -> str:
    values = list(argv) if argv is not None else sys.argv[1:]
    return "python scripts/run_canonical_max_depth_universe_v2.py " + " ".join(
        shlex.quote(value) for value in values
    )


def _aggregate(rows: list[dict[str, Any]], cutoff: datetime) -> dict[str, Any]:
    full = outcome_counts(rows)
    development = outcome_counts(
        [row for row in rows if utc(row["activated_at_utc"]) < cutoff]
    )
    holdout = outcome_counts(
        [row for row in rows if utc(row["activated_at_utc"]) >= cutoff]
    )
    groups = []
    dimensions = {
        "symbol": lambda row: row.get("symbol"),
        "family": lambda row: row.get("setup_family"),
        "direction": lambda row: row.get("direction"),
        "session": lambda row: row.get("session_id"),
        "year": lambda row: utc(row["activated_at_utc"]).year,
        "identity": lambda row: row.get("candidate_id"),
        "outcome": lambda row: row.get("outcome"),
    }
    for dimension, getter in dimensions.items():
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            buckets[str(getter(row) or "UNKNOWN")].append(row)
        for value, bucket in sorted(buckets.items()):
            groups.append(
                {"dimension": dimension, "value": value, **outcome_counts(bucket)}
            )
    return {
        "full": full,
        "development": development,
        "holdout": holdout,
        "ready_count": sum(bool(row.get("ready")) for row in rows),
        "fill_count": sum(bool(row.get("filled_at_utc")) for row in rows),
        "groups": groups,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    symbols, start, end, data_cutoff = validate_args(args)
    provider = DukascopyMaxDepthProvider()
    run_dir = args.output_root
    environment = check_environment(run_dir, args.cache_root)
    code_revision = code_sha()
    base = {
        "version": VERSION,
        "provider_profile": provider.profile_name,
        "provider_profile_hash": provider.profile_hash(),
        "source_policy": provider.policy.to_dict(),
        "symbols": symbols,
        "data_cutoff_utc": data_cutoff.isoformat(),
        "holdout_cutoff_utc": utc(args.holdout_cutoff_utc).isoformat()
        if args.holdout_cutoff_utc
        else None,
        "exact_cli_invocation": _invocation(argv),
        "code_sha": code_revision,
        "cache_root": str(args.cache_root.resolve()),
        "restore_instructions": "Restore the raw cache separately, then run --verify-only before replay. Back it up before Entry Timing, Daily+Asia, and Exit Model research.",
    }
    requests = [
        HistoricalM5Request(symbol, start, end, data_cutoff, args.cache_root)
        for symbol in symbols
    ]
    if args.plan_only:
        plan = {
            **base,
            "mode": "PLAN_ONLY",
            "network_used": False,
            "partition_counts": {
                request.symbol: len(provider.plan(request)) for request in requests
            },
        }
        write_json(run_dir / "audit_manifest.json", plan)
        return 0
    if args.download_only or (not args.replay_only and not args.verify_only):
        provider.download()
    cache_audits = {
        request.symbol: provider.verify_cache(request) for request in requests
    }
    if not all(value["cache_complete"] for value in cache_audits.values()):
        raise CanonicalRunnerError("raw cache is incomplete")
    source_hashes = [
        part["source_sha256"]
        for audit in cache_audits.values()
        for part in audit["partitions"]
    ]
    if args.verify_only:
        write_json(
            run_dir / "audit_manifest.json",
            {**base, "mode": "VERIFY_ONLY", "cache_completeness": True},
        )
        return 0

    all_events: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    all_candidates: list[HistoricalWatchCandidate] = []
    coverage: list[dict[str, Any]] = []
    for symbol in symbols:
        history_path = args.cache_root / "history" / f"{symbol}_5m.parquet"
        if not history_path.is_file():
            raise CanonicalRunnerError(
                f"normalized history is missing for symbol={symbol}"
            )
        history = normalize_m5_history(pd.read_parquet(history_path), symbol=symbol)
        event_candidates, audit = reconstruct_tpo_watch_candidates(
            history, symbol=symbol, include_counter_htf_events=True
        )
        candidates = [
            candidate
            for candidate in event_candidates
            if candidate.payload.get("event_census_execution_eligible") is True
        ]
        events = [
            enrich_event(
                candidate,
                measure_event_development(
                    candidate,
                    history,
                    primary_development_r=args.primary_development_r,
                    thresholds_r=DEVELOPMENT_THRESHOLDS,
                ),
                history,
            )
            for candidate in event_candidates
        ]
        rows = [
            enrich_execution(candidate, replay_candidate(candidate, history), history)
            for candidate in candidates
        ]
        all_events.extend(events)
        all_rows.extend(rows)
        all_candidates.extend(candidates)
        coverage.append(audit)
    holdout_cutoff = (
        utc(args.holdout_cutoff_utc)
        if args.holdout_cutoff_utc
        else derive_holdout_cutoff(all_events, args.holdout_fraction)
    )
    final_base = {**base, "holdout_cutoff_utc": holdout_cutoff.isoformat()}
    write_json(
        run_dir / "raw_source_coverage_manifest.json",
        {**final_base, "cache_completeness": True, "symbols": cache_audits},
    )
    write_json(
        run_dir / "raw_source_hash_manifest.json",
        {**final_base, "source_hashes": source_hashes},
    )
    for row in all_events:
        row["holdout_cohort"] = (
            "DEVELOPMENT"
            if utc(row["confirmed_at_utc"]) < holdout_cutoff
            else "HOLDOUT"
        )
    for row in all_rows:
        row["holdout_cohort"] = (
            "DEVELOPMENT"
            if utc(row["activated_at_utc"]) < holdout_cutoff
            else "HOLDOUT"
        )
    event_frame = pd.DataFrame(
        sorted(
            all_events, key=lambda row: (row["confirmed_at_utc"], row["candidate_id"])
        )
    )
    execution_frame = pd.DataFrame(
        sorted(all_rows, key=lambda row: (row["activated_at_utc"], row["candidate_id"]))
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    event_path = run_dir / "otd_orr_event_census_v2.parquet"
    event_frame.to_parquet(event_path, index=False)
    execution_path = run_dir / "execution_candidates_v2.parquet"
    execution_frame.to_parquet(execution_path, index=False)
    event_csv = run_dir / "otd_orr_event_census_v2_compact.csv.gz"
    write_compact_csv_gz(event_csv, event_frame, EVENT_COMPACT_COLUMNS)
    execution_csv = run_dir / "execution_candidates_v2_compact.csv.gz"
    write_compact_csv_gz(execution_csv, execution_frame, EXECUTION_COMPACT_COLUMNS)
    identities = {
        "otd_orr_event_census_v2": {
            **semantic_identity(
                event_frame,
                id_column="candidate_id",
                timestamp_column="confirmed_at_utc",
                code_revision=code_revision,
                environment_versions=environment["versions"],
                source_hashes=source_hashes,
                data_cutoff_utc=data_cutoff.isoformat(),
                holdout_cutoff_utc=holdout_cutoff.isoformat(),
            ),
            "file_sha256": sha256_file(event_path),
        },
        "execution_candidates_v2": {
            **semantic_identity(
                execution_frame,
                id_column="candidate_id",
                timestamp_column="activated_at_utc",
                code_revision=code_revision,
                environment_versions=environment["versions"],
                source_hashes=source_hashes,
                data_cutoff_utc=data_cutoff.isoformat(),
                holdout_cutoff_utc=holdout_cutoff.isoformat(),
            ),
            "file_sha256": sha256_file(execution_path),
        },
    }
    aggregate = _aggregate(all_rows, holdout_cutoff)
    full_universe = tuple(symbols) == CANONICAL_SYMBOLS
    bundle_type = "CANONICAL_FROZEN_UNIVERSE" if full_universe else "DIAGNOSTIC_SUBSET"
    universe_frozen = False  # becomes true only after ZIP and restore verification
    write_json(run_dir / "environment_manifest.json", environment)
    write_json(
        run_dir / "provider_mapping_manifest.json",
        {**final_base, "mapping": provider.symbol_mapping},
    )
    write_json(run_dir / "semantic_hash_manifest.json", identities)
    write_json(run_dir / "aggregate_summary.json", aggregate)
    summary_rows = [
        {"scope": "cohort", "cohort": key, **value}
        for key, value in aggregate.items()
        if key in {"full", "development", "holdout"}
    ]
    summary_rows.extend({"scope": "dimension", **row} for row in aggregate["groups"])
    pd.DataFrame(summary_rows).to_csv(run_dir / "aggregate_summary.csv", index=False)
    reconciliation = []
    for cohort in ("full", "development", "holdout"):
        for metric, historical in HISTORICAL_REFERENCE[cohort].items():
            current = aggregate[cohort][metric]
            reconciliation.append(
                {
                    "cohort": cohort,
                    "dimension": "ALL",
                    "metric": metric,
                    "historical_value": historical,
                    "current_value": current,
                    "delta": current - historical if current is not None else None,
                }
            )
    for group in aggregate["groups"]:
        for metric in ("tp_count", "sl_count", "resolved_count", "win_rate"):
            reconciliation.append(
                {
                    "cohort": "full",
                    "dimension": group["dimension"],
                    "dimension_value": group["value"],
                    "metric": metric,
                    "historical_value": None,
                    "current_value": group[metric],
                    "delta": None,
                    "reference_available": False,
                    "status": "HISTORICAL_DETAIL_UNAVAILABLE",
                }
            )
    pd.DataFrame(reconciliation).to_csv(
        run_dir / "historical_reconciliation.csv", index=False
    )
    (run_dir / "historical_discrepancy_report.md").write_text(
        "# Historical discrepancy report\n\nThe lost report is a comparison reference, not a target. Methodology was not changed to reproduce its counts.\n"
    )
    (run_dir / "canonical_max_depth_v2_report.md").write_text(
        f"# Canonical Max-Depth Universe v2\n\n- Bundle type: `{bundle_type}`\n- Universe frozen: `{str(universe_frozen).lower()}`\n- Data cutoff: `{data_cutoff.isoformat()}`\n- Holdout cutoff: `{holdout_cutoff.isoformat()}`\n- Provider parity: `{provider.policy.parity_status}`\n"
    )
    frozen = {
        **final_base,
        "holdout_cutoff_source": "EXPLICIT"
        if args.holdout_cutoff_utc
        else "DERIVED_ONCE_FROM_FULL_EVENT_UNIVERSE",
        "bundle_type": bundle_type,
        "universe_frozen": universe_frozen,
        "cache_completeness": True,
    }
    write_json(run_dir / "frozen_universe_v2_manifest.json", frozen)
    write_json(
        run_dir / "audit_manifest.json",
        {
            **frozen,
            "canonical_datasets_valid": True,
            "hashes_valid": True,
            "zip_verified": False,
            "restore_verified": False,
        },
    )
    if args.build_zip:
        if not full_universe:
            raise CanonicalRunnerError(
                "diagnostic subsets cannot create the canonical ZIP"
            )
        raise CanonicalRunnerError(
            "universe_frozen requires external restore verification before canonical ZIP publication"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
