from __future__ import annotations

"""Research-only canonical max-depth universe v2 orchestration.

This entry point is intentionally separate from the Twelve Data/Yahoo runner.
It supports the existing tick-aggregated diagnostic profile and the offline
reader for source chunks exported through the native JForex M5 BID SDK path.
"""

import argparse
import json
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
from app.services.otd_orr_event_census import (
    compile_event_census,
    measure_event_development,
)
from app.services.research.canonical_universe_artifacts import (
    code_sha,
    outcome_counts,
    semantic_identity,
    sha256_file,
    write_compact_csv_gz,
    write_csv,
    write_json,
    write_parquet,
    write_text,
)
from app.services.research.dukascopy_max_depth_provider import (
    PROFILE_NAME as TICK_PROFILE_NAME,
    DukascopyMaxDepthProvider,
)
from app.services.research.historical_m5_provider import HistoricalM5Request
from app.services.research.jforex_native_m5_provider import (
    CANONICAL_SYMBOLS as JFOREX_CANONICAL_SYMBOLS,
    PROFILE_NAME as JFOREX_PROFILE_NAME,
    JForexNativeM5Provider,
    qualifies_for_canonical_native_parity,
)
from scripts.check_research_environment import check_environment

VERSION = "canonical-max-depth-universe-v2.0.0"
CANONICAL_SYMBOLS = JFOREX_CANONICAL_SYMBOLS
SUPPORTED_PROVIDER_PROFILES = (TICK_PROFILE_NAME, JFOREX_PROFILE_NAME)
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
    "trade_first_0_5R_hit_at_utc",
    "trade_first_1R_hit_at_utc",
    "trade_first_1_5R_hit_at_utc",
    "trade_first_2R_hit_at_utc",
    "trade_first_2_5R_hit_at_utc",
    "trade_first_3R_hit_at_utc",
    "trade_stop_hit_at_utc",
    "trade_real_target_hit_at_utc",
    "trade_MFE_R",
    "trade_MAE_R",
    "trade_same_bar_ambiguity",
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
    events: Sequence[Mapping[str, Any]],
    executions: Sequence[Mapping[str, Any]],
    fraction: float,
    primary_development_r: float = 1.5,
) -> tuple[datetime | None, dict[str, Any]]:
    census = compile_event_census(
        event_records=events,
        execution_rows=executions,
        holdout_fraction=fraction,
        primary_development_r=primary_development_r,
    )
    value = census["holdout_start_utc"]
    return (utc(value) if value else None), census


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
    closes = pd.to_datetime(history["bar_close_utc"], utc=True)
    activation = history.loc[closes == pd.Timestamp(candidate.activated_at_utc)]
    event_reference = (
        float(activation.iloc[-1]["close"]) if not activation.empty else None
    )
    event_risk = (
        abs(event_reference - candidate.test_extreme)
        if event_reference is not None
        else 0.0
    )
    target_price = result.get("target_reference_price")
    event_target_r = None
    if target_price is not None and event_risk > 0:
        event_target_r = (
            (float(target_price) - event_reference) / event_risk
            if candidate.direction == "LONG"
            else (event_reference - float(target_price)) / event_risk
        )
    result.update(
        {
            "event_progress_R_at_ready": ready_progress,
            "event_progress_R_at_fill": fill_progress,
            "max_event_R_before_ready": ready_progress,
            "max_event_R_before_fill": fill_progress,
            "remaining_target_R_at_ready": max(0.0, event_target_r - ready_progress)
            if event_target_r is not None and ready_progress is not None
            else None,
            "remaining_target_R_at_fill": max(0.0, event_target_r - fill_progress)
            if event_target_r is not None and fill_progress is not None
            else None,
            "remaining_target_fraction_at_ready": max(
                0.0, event_target_r - ready_progress
            )
            / event_target_r
            if event_target_r and ready_progress is not None
            else None,
            "remaining_target_fraction_at_fill": max(
                0.0, event_target_r - fill_progress
            )
            / event_target_r
            if event_target_r and fill_progress is not None
            else None,
        }
    )
    result.update(_trade_path(result, history, candidate.direction))
    return result


def _trade_path(
    row: Mapping[str, Any], history: pd.DataFrame, direction: str
) -> dict[str, Any]:
    names = {0.5: "0_5", 1.0: "1", 1.5: "1_5", 2.0: "2", 2.5: "2_5", 3.0: "3"}
    output = {f"trade_first_{name}R_hit_at_utc": None for name in names.values()}
    output.update(
        {
            "trade_stop_hit_at_utc": None,
            "trade_real_target_hit_at_utc": None,
            "trade_MFE_R": None,
            "trade_MAE_R": None,
            "trade_same_bar_ambiguity": False,
        }
    )
    if not row.get("filled_at_utc"):
        return output
    entry, stop, target = (
        row.get(key)
        for key in (
            "entry_reference_price",
            "invalidation_reference_price",
            "target_reference_price",
        )
    )
    if (
        entry is None
        or stop is None
        or target is None
        or abs(float(entry) - float(stop)) <= 0
    ):
        return output
    entry, stop, target = float(entry), float(stop), float(target)
    risk = abs(entry - stop)
    end = row.get("trade_resolution_expires_at_utc") or row.get("expires_at_utc")
    closes = pd.to_datetime(history["bar_close_utc"], utc=True)
    bars = history.loc[
        (closes > pd.Timestamp(utc(row["filled_at_utc"])))
        & (closes <= pd.Timestamp(utc(end)))
    ]
    mfe = mae = 0.0
    for _, bar in bars.iterrows():
        at = utc(bar["bar_close_utc"]).isoformat()
        favorable = (
            (float(bar["high"]) - entry) / risk
            if direction == "LONG"
            else (entry - float(bar["low"])) / risk
        )
        adverse = (
            (entry - float(bar["low"])) / risk
            if direction == "LONG"
            else (float(bar["high"]) - entry) / risk
        )
        mfe, mae = max(mfe, favorable), max(mae, adverse)
        stop_hit = (
            float(bar["low"]) <= stop
            if direction == "LONG"
            else float(bar["high"]) >= stop
        )
        target_hit = (
            float(bar["high"]) >= target
            if direction == "LONG"
            else float(bar["low"]) <= target
        )
        if stop_hit and output["trade_stop_hit_at_utc"] is None:
            output["trade_stop_hit_at_utc"] = at
        if target_hit and output["trade_real_target_hit_at_utc"] is None:
            output["trade_real_target_hit_at_utc"] = at
        if stop_hit and target_hit:
            output["trade_same_bar_ambiguity"] = True
        for threshold, name in names.items():
            key = f"trade_first_{name}R_hit_at_utc"
            if output[key] is None and favorable >= threshold and not stop_hit:
                output[key] = at
    output["trade_MFE_R"], output["trade_MAE_R"] = mfe, mae
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the research-only canonical max-depth universe v2"
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--provider-profile", default=TICK_PROFILE_NAME)
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
    if args.max_workers != 1:
        raise CanonicalRunnerError(
            "NOT_IMPLEMENTED: --max-workers requires authoritative downloader"
        )
    if args.resume:
        raise CanonicalRunnerError(
            "NOT_IMPLEMENTED: --resume requires authoritative downloader"
        )
    if args.provider_profile not in SUPPORTED_PROVIDER_PROFILES:
        raise CanonicalRunnerError(
            "provider profile must be an implemented research profile"
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
    if args.holdout_cutoff_utc:
        explicit_holdout = utc(args.holdout_cutoff_utc)
        if not start < explicit_holdout < cutoff:
            raise CanonicalRunnerError(
                "explicit holdout cutoff must be strictly inside the replay window"
            )
    if cutoff.minute % 5 or cutoff.second or cutoff.microsecond:
        raise CanonicalRunnerError("data cutoff must be a fully closed M5 boundary")
    return symbols, start, end, cutoff


def _invocation(argv: Sequence[str] | None) -> str:
    values = list(argv) if argv is not None else sys.argv[1:]
    return "python scripts/run_canonical_max_depth_universe_v2.py " + " ".join(
        shlex.quote(value) for value in values
    )


def _provider_for_profile(profile: str) -> Any:
    if profile == JFOREX_PROFILE_NAME:
        return JForexNativeM5Provider()
    if profile == TICK_PROFILE_NAME:
        return DukascopyMaxDepthProvider()
    raise CanonicalRunnerError("unsupported research provider profile")


def verify_canonical_artifacts(run_dir: Path) -> dict[str, Any]:
    required = {
        "otd_orr_event_census_v2.parquet",
        "execution_candidates_v2.parquet",
        "semantic_hash_manifest.json",
        "aggregate_summary.json",
        "frozen_universe_v2_manifest.json",
        "audit_manifest.json",
    }
    missing = sorted(name for name in required if not (run_dir / name).is_file())
    if missing:
        raise CanonicalRunnerError(f"canonical verify-only missing files: {missing}")
    event_frame = pd.read_parquet(run_dir / "otd_orr_event_census_v2.parquet")
    execution_frame = pd.read_parquet(run_dir / "execution_candidates_v2.parquet")
    identities = json.loads(
        (run_dir / "semantic_hash_manifest.json").read_text(encoding="utf-8")
    )
    frozen = json.loads(
        (run_dir / "frozen_universe_v2_manifest.json").read_text(encoding="utf-8")
    )
    for name, frame, timestamp in (
        ("otd_orr_event_census_v2", event_frame, "confirmed_at_utc"),
        ("execution_candidates_v2", execution_frame, "activated_at_utc"),
    ):
        expected = identities[name]
        path = run_dir / f"{name}.parquet"
        if sha256_file(path) != expected["file_sha256"]:
            raise CanonicalRunnerError("canonical file hash mismatch")
        actual = semantic_identity(
            frame,
            id_column="candidate_id",
            timestamp_column=timestamp,
            code_revision=expected["code_sha"],
            environment_versions=expected["environment_versions"],
            source_hashes=expected["source_hashes"],
            data_cutoff_utc=expected["data_cutoff_utc"],
            holdout_cutoff_utc=expected["holdout_cutoff_utc"],
        )
        for key in (
            "semantic_content_sha256",
            "schema_hash",
            "ordered_id_hash",
            "row_count",
            "unique_id_count",
        ):
            if actual[key] != expected[key]:
                raise CanonicalRunnerError(f"canonical identity mismatch: {name}:{key}")
    event_ids = set(event_frame["candidate_id"].astype(str))
    if not set(execution_frame["candidate_id"].astype(str)).issubset(event_ids):
        raise CanonicalRunnerError(
            "execution candidate does not join exactly one event"
        )
    aggregate = json.loads(
        (run_dir / "aggregate_summary.json").read_text(encoding="utf-8")
    )
    cutoff = frozen.get("holdout_cutoff_utc")
    restored = _aggregate(
        execution_frame.to_dict(orient="records"), utc(cutoff) if cutoff else None
    )
    if restored != aggregate:
        raise CanonicalRunnerError("restored aggregate mismatch")
    return {"mode": "VERIFY_ONLY", "canonical_artifacts_valid": True}


def _aggregate(rows: list[dict[str, Any]], cutoff: datetime | None) -> dict[str, Any]:
    full = outcome_counts(rows)
    development = outcome_counts(
        rows
        if cutoff is None
        else [row for row in rows if utc(row["activated_at_utc"]) < cutoff]
    )
    holdout = outcome_counts(
        []
        if cutoff is None
        else [row for row in rows if utc(row["activated_at_utc"]) >= cutoff]
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
    provider = _provider_for_profile(args.provider_profile)
    run_dir = args.output_root
    environment = check_environment(run_dir, args.cache_root)
    code_revision = code_sha()
    base = {
        "version": VERSION,
        "provider_profile": provider.profile_name,
        "provider_profile_hash": provider.profile_hash(),
        "source_policy": provider.policy.to_dict(),
        "symbols": symbols,
        "canonical_native_parity_candidate": qualifies_for_canonical_native_parity(
            provider, symbols
        ),
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
    if args.verify_only:
        result = verify_canonical_artifacts(run_dir)
        write_json(run_dir / "verification_result.json", {**base, **result})
        return 0
    acquisition_mode = args.download_only or (
        not args.replay_only and not args.verify_only
    )
    if acquisition_mode and not args.allow_network_fetch:
        raise CanonicalRunnerError(
            "--allow-network-fetch is required before acquisition"
        )
    if acquisition_mode:
        provider.download()
    full_universe_requested = tuple(symbols) == CANONICAL_SYMBOLS
    if isinstance(provider, JForexNativeM5Provider):
        cache_audits = provider.verify_universe(
            requests, require_full_universe=full_universe_requested
        )
    else:
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
    source_hashes.extend(
        audit["manifest_sha256"]
        for audit in cache_audits.values()
        if audit.get("manifest_sha256")
    )
    if args.download_only:
        write_json(
            run_dir / "raw_source_coverage_manifest.json",
            {**base, "mode": "DOWNLOAD_ONLY", "symbols": cache_audits},
        )
        write_json(
            run_dir / "raw_source_hash_manifest.json",
            {**base, "source_hashes": source_hashes},
        )
        return 0

    all_events: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    all_candidates: list[HistoricalWatchCandidate] = []
    coverage: list[dict[str, Any]] = []
    request_by_symbol = {request.symbol: request for request in requests}
    for symbol in symbols:
        if isinstance(provider, JForexNativeM5Provider):
            source_history = provider.load_history(request_by_symbol[symbol])
        else:
            history_path = args.cache_root / "history" / f"{symbol}_5m.parquet"
            if not history_path.is_file():
                raise CanonicalRunnerError(
                    f"normalized history is missing for symbol={symbol}"
                )
            source_history = pd.read_parquet(history_path)
        history = normalize_m5_history(source_history, symbol=symbol)
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
    if args.holdout_cutoff_utc:
        holdout_cutoff = utc(args.holdout_cutoff_utc)
        event_times = [utc(row["confirmed_at_utc"]) for row in all_events]
        if not event_times or not min(event_times) < holdout_cutoff <= max(event_times):
            raise CanonicalRunnerError(
                "explicit holdout cutoff is outside the event universe"
            )
        census_split = {
            "holdout_start_utc": holdout_cutoff.isoformat(),
            "holdout_status": "EXPLICIT_VALIDATED",
            "realized_event_holdout_fraction": sum(
                timestamp >= holdout_cutoff for timestamp in event_times
            )
            / len(event_times),
            "realized_execution_holdout_fraction": sum(
                utc(row["activated_at_utc"]) >= holdout_cutoff for row in all_rows
            )
            / len(all_rows)
            if all_rows
            else None,
        }
    else:
        holdout_cutoff, census_split = derive_holdout_cutoff(
            all_events,
            all_rows,
            args.holdout_fraction,
            args.primary_development_r,
        )
    final_base = {
        **base,
        "holdout_cutoff_utc": holdout_cutoff.isoformat() if holdout_cutoff else None,
        "holdout_status": census_split["holdout_status"],
        "realized_event_holdout_fraction": census_split[
            "realized_event_holdout_fraction"
        ],
        "realized_execution_holdout_fraction": census_split[
            "realized_execution_holdout_fraction"
        ],
    }
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
            if holdout_cutoff is None or utc(row["confirmed_at_utc"]) < holdout_cutoff
            else "HOLDOUT"
        )
    for row in all_rows:
        row["holdout_cohort"] = (
            "DEVELOPMENT"
            if holdout_cutoff is None or utc(row["activated_at_utc"]) < holdout_cutoff
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
    write_parquet(event_path, event_frame)
    execution_path = run_dir / "execution_candidates_v2.parquet"
    write_parquet(execution_path, execution_frame)
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
                holdout_cutoff_utc=holdout_cutoff.isoformat()
                if holdout_cutoff
                else "UNAVAILABLE",
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
                holdout_cutoff_utc=holdout_cutoff.isoformat()
                if holdout_cutoff
                else "UNAVAILABLE",
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
    restored_events = pd.read_parquet(event_path)
    restored_executions = pd.read_parquet(execution_path)
    if not set(restored_executions["candidate_id"].astype(str)).issubset(
        set(restored_events["candidate_id"].astype(str))
    ):
        raise CanonicalRunnerError("restored execution/event identity join failed")
    for name, restored, timestamp in (
        ("otd_orr_event_census_v2", restored_events, "confirmed_at_utc"),
        ("execution_candidates_v2", restored_executions, "activated_at_utc"),
    ):
        restored_identity = semantic_identity(
            restored,
            id_column="candidate_id",
            timestamp_column=timestamp,
            code_revision=code_revision,
            environment_versions=environment["versions"],
            source_hashes=source_hashes,
            data_cutoff_utc=data_cutoff.isoformat(),
            holdout_cutoff_utc=holdout_cutoff.isoformat()
            if holdout_cutoff
            else "UNAVAILABLE",
        )
        for key in ("semantic_content_sha256", "schema_hash", "ordered_id_hash"):
            if restored_identity[key] != identities[name][key]:
                raise CanonicalRunnerError(
                    f"post-write identity validation failed: {name}:{key}"
                )
    if (
        _aggregate(restored_executions.to_dict(orient="records"), holdout_cutoff)
        != aggregate
    ):
        raise CanonicalRunnerError("post-write aggregate validation failed")
    summary_rows = [
        {"scope": "cohort", "cohort": key, **value}
        for key, value in aggregate.items()
        if key in {"full", "development", "holdout"}
    ]
    summary_rows.extend({"scope": "dimension", **row} for row in aggregate["groups"])
    write_csv(run_dir / "aggregate_summary.csv", pd.DataFrame(summary_rows))
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
    write_csv(run_dir / "historical_reconciliation.csv", pd.DataFrame(reconciliation))
    write_text(
        run_dir / "historical_discrepancy_report.md",
        "# Historical discrepancy report\n\nThe lost report is a comparison reference, not a target. Methodology was not changed to reproduce its counts.\n",
    )
    write_text(
        run_dir / "canonical_max_depth_v2_report.md",
        f"# Canonical Max-Depth Universe v2\n\n- Bundle type: `{bundle_type}`\n- Universe frozen: `{str(universe_frozen).lower()}`\n- Data cutoff: `{data_cutoff.isoformat()}`\n- Holdout cutoff: `{holdout_cutoff.isoformat() if holdout_cutoff else 'unavailable'}`\n- Provider parity: `{provider.policy.parity_status}`\n",
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
        if not qualifies_for_canonical_native_parity(provider, symbols):
            raise CanonicalRunnerError(
                "PROVIDER_PARITY_NOT_APPROVED: non-parity provider cannot create canonical ZIP"
            )
        raise CanonicalRunnerError(
            "CANONICAL_PUBLICATION_GATES_REQUIRED: run deterministic second-run and "
            "restore verification before creating a frozen-universe ZIP"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
