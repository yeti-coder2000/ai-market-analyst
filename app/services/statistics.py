from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd


DEFAULT_JOURNAL_PATH = Path("runtime/radar_journal.ndjson")
DEFAULT_SIGNAL_RECORDS_JSON_PATH = Path("runtime/stats/signals_flat.json")
DEFAULT_SIGNAL_RECORDS_PARQUET_PATH = Path("runtime/stats/signals_flat.parquet")
DEFAULT_DAILY_SUMMARY_PATH = Path("runtime/stats/daily_summary.json")


@dataclass
class SignalRecord:
    signal_id: str
    symbol: str
    timeframe: str
    cycle_id: str
    created_at_utc: str

    scenario: str
    signal_class: str
    direction: str
    market_state: str
    htf_bias: str
    confidence: float

    entry_reference_price: Optional[float]
    invalidation_reference_price: Optional[float]
    target_reference_price: Optional[float]

    execution_status: Optional[str] = None
    execution_model: Optional[str] = None
    risk_reward_ratio: Optional[float] = None
    stop_distance: Optional[float] = None
    target_distance: Optional[float] = None
    execution_timeframe: Optional[str] = None
    trigger_reason: Optional[str] = None

    was_sent_to_telegram: bool = False
    was_deduped: bool = False

    current_stage: str = "SCENARIO_FORMING"
    final_status: Optional[str] = None
    resolution_reason: Optional[str] = None

    bars_alive: int = 0
    minutes_alive: int = 0
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    time_to_validation_min: Optional[int] = None

    runner_version: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _confidence_bucket(confidence: float) -> str:
    if confidence < 0.30:
        return "0.00-0.29"
    if confidence < 0.40:
        return "0.30-0.39"
    if confidence < 0.50:
        return "0.40-0.49"
    if confidence < 0.60:
        return "0.50-0.59"
    return "0.60+"


def load_journal_events(path: Path | str = DEFAULT_JOURNAL_PATH) -> list[dict]:
    path = Path(path)
    if not path.exists():
        return []

    events: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                item = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if isinstance(item, dict) and "event_type" in item:
                events.append(item)

    return events


def build_signal_records(events: list[dict]) -> list[SignalRecord]:
    """
    Builds flat signal records from event log.

    Expected event_type:
    - signal_candidate_detected
    - signal_registered
    - signal_updated
    - signal_resolved
    - signal_deduped
    - telegram_sent
    """
    records: dict[str, SignalRecord] = {}

    for event in events:
        event_type = event.get("event_type")
        payload = event.get("payload", {}) or {}

        if event_type == "signal_candidate_detected":
            continue

        if event_type == "signal_registered":
            signal_id = payload.get("signal_id")
            if not signal_id:
                continue

            if signal_id not in records:
                records[signal_id] = SignalRecord(
                    signal_id=signal_id,
                    symbol=payload.get("symbol", event.get("symbol", "-")),
                    timeframe=event.get("timeframe", "15m"),
                    cycle_id=event.get("cycle_id", ""),
                    created_at_utc=payload.get("created_at_utc", event.get("ts_utc", "")),
                    scenario=payload.get("scenario", "UNKNOWN"),
                    signal_class=payload.get("signal_class", "SCENARIO_FORMING"),
                    direction=payload.get("direction", "NEUTRAL"),
                    market_state=payload.get("market_state", ""),
                    htf_bias=payload.get("htf_bias", ""),
                    confidence=_safe_float(payload.get("confidence"), 0.0) or 0.0,
                    entry_reference_price=_safe_float(payload.get("entry_reference_price"), None),
                    invalidation_reference_price=_safe_float(
                        payload.get("invalidation_reference_price"),
                        None,
                    ),
                    target_reference_price=_safe_float(
                        payload.get("target_reference_price"),
                        None,
                    ),
                    execution_status=payload.get("execution_status"),
                    execution_model=payload.get("execution_model"),
                    risk_reward_ratio=_safe_float(payload.get("risk_reward_ratio"), None),
                    stop_distance=_safe_float(payload.get("stop_distance"), None),
                    target_distance=_safe_float(payload.get("target_distance"), None),
                    execution_timeframe=payload.get("execution_timeframe"),
                    trigger_reason=payload.get("trigger_reason"),
                    current_stage=payload.get(
                        "current_stage",
                        payload.get("signal_class", "SCENARIO_FORMING"),
                    ),
                    runner_version=event.get("runner_version"),
                )
            continue

        if event_type == "signal_updated":
            signal_id = payload.get("signal_id")
            if not signal_id or signal_id not in records:
                continue

            rec = records[signal_id]
            updated_payload = payload.get("payload", {}) or {}

            rec.current_stage = updated_payload.get("signal_class", rec.current_stage)
            rec.signal_class = updated_payload.get("signal_class", rec.signal_class)
            rec.confidence = (
                _safe_float(updated_payload.get("confidence"), rec.confidence)
                or rec.confidence
            )

            rec.entry_reference_price = _safe_float(
                updated_payload.get("entry_reference_price"),
                rec.entry_reference_price,
            )
            rec.invalidation_reference_price = _safe_float(
                updated_payload.get("invalidation_reference_price"),
                rec.invalidation_reference_price,
            )
            rec.target_reference_price = _safe_float(
                updated_payload.get("target_reference_price"),
                rec.target_reference_price,
            )

            rec.execution_status = updated_payload.get(
                "execution_status",
                rec.execution_status,
            )
            rec.execution_model = updated_payload.get(
                "execution_model",
                rec.execution_model,
            )
            rec.risk_reward_ratio = _safe_float(
                updated_payload.get("risk_reward_ratio"),
                rec.risk_reward_ratio,
            )
            rec.stop_distance = _safe_float(
                updated_payload.get("stop_distance"),
                rec.stop_distance,
            )
            rec.target_distance = _safe_float(
                updated_payload.get("target_distance"),
                rec.target_distance,
            )
            rec.execution_timeframe = updated_payload.get(
                "execution_timeframe",
                rec.execution_timeframe,
            )
            rec.trigger_reason = updated_payload.get(
                "trigger_reason",
                rec.trigger_reason,
            )

            rec.scenario = updated_payload.get("scenario", rec.scenario)
            rec.direction = updated_payload.get("direction", rec.direction)
            rec.market_state = updated_payload.get("market_state", rec.market_state)
            continue

        if event_type == "signal_resolved":
            signal_id = payload.get("signal_id")
            if not signal_id or signal_id not in records:
                continue

            rec = records[signal_id]

            rec.signal_class = payload.get("signal_class", rec.signal_class)
            rec.current_stage = payload.get("signal_class", rec.current_stage)

            rec.final_status = payload.get("resolution")
            rec.resolution_reason = payload.get("resolution_note")

            rec.bars_alive = _safe_int(payload.get("bars_alive"), rec.bars_alive)
            rec.minutes_alive = _safe_int(payload.get("minutes_alive"), rec.minutes_alive)
            rec.mfe_pct = _safe_float(payload.get("mfe_pct"), rec.mfe_pct) or rec.mfe_pct
            rec.mae_pct = _safe_float(payload.get("mae_pct"), rec.mae_pct) or rec.mae_pct
            rec.time_to_validation_min = payload.get(
                "time_to_validation_min",
                rec.time_to_validation_min,
            )

            rec.entry_reference_price = _safe_float(
                payload.get("entry_reference_price"),
                rec.entry_reference_price,
            )
            rec.invalidation_reference_price = _safe_float(
                payload.get("invalidation_reference_price"),
                rec.invalidation_reference_price,
            )
            rec.target_reference_price = _safe_float(
                payload.get("target_reference_price"),
                rec.target_reference_price,
            )
            rec.execution_status = payload.get("execution_status", rec.execution_status)
            rec.execution_model = payload.get("execution_model", rec.execution_model)
            rec.risk_reward_ratio = _safe_float(
                payload.get("risk_reward_ratio"),
                rec.risk_reward_ratio,
            )
            rec.stop_distance = _safe_float(
                payload.get("stop_distance"),
                rec.stop_distance,
            )
            rec.target_distance = _safe_float(
                payload.get("target_distance"),
                rec.target_distance,
            )
            rec.execution_timeframe = payload.get(
                "execution_timeframe",
                rec.execution_timeframe,
            )
            rec.trigger_reason = payload.get("trigger_reason", rec.trigger_reason)
            continue

        if event_type == "signal_deduped":
            continue

        if event_type == "telegram_sent":
            signal_id = payload.get("signal_id")
            if not signal_id or signal_id not in records:
                continue

            rec = records[signal_id]
            rec.was_sent_to_telegram = True
            continue

    return list(records.values())


def compute_system_metrics(events: list[dict]) -> dict:
    cycles_total = 0
    cycles_ok = 0
    cycles_with_errors = 0

    instrument_analyzed_total = 0
    signal_registered_total = 0
    signal_updated_total = 0
    signal_resolved_total = 0
    signal_deduped_total = 0
    telegram_sent_total = 0
    telegram_failed_total = 0

    total_duration_sec = 0.0
    finished_cycles_count = 0

    for event in events:
        et = event.get("event_type")
        status = event.get("status")
        payload = event.get("payload", {}) or {}

        if et == "cycle_finished":
            cycles_total += 1
            finished_cycles_count += 1
            total_duration_sec += _safe_float(payload.get("duration_sec"), 0.0) or 0.0
            if status == "ok":
                cycles_ok += 1
            else:
                cycles_with_errors += 1

        elif et == "instrument_analyzed":
            instrument_analyzed_total += 1
        elif et == "signal_registered":
            signal_registered_total += 1
        elif et == "signal_updated":
            signal_updated_total += 1
        elif et == "signal_resolved":
            signal_resolved_total += 1
        elif et == "signal_deduped":
            signal_deduped_total += 1
        elif et == "telegram_sent":
            telegram_sent_total += 1
        elif et == "telegram_failed":
            telegram_failed_total += 1

    avg_cycle_duration_sec = round(
        total_duration_sec / finished_cycles_count,
        3,
    ) if finished_cycles_count else 0.0

    telegram_send_success_rate = round(
        telegram_sent_total / (telegram_sent_total + telegram_failed_total),
        4,
    ) if (telegram_sent_total + telegram_failed_total) else 0.0

    return {
        "cycles_total": cycles_total,
        "cycles_ok": cycles_ok,
        "cycles_with_errors": cycles_with_errors,
        "avg_cycle_duration_sec": avg_cycle_duration_sec,
        "instrument_analyzed_total": instrument_analyzed_total,
        "signal_registered_total": signal_registered_total,
        "signal_updated_total": signal_updated_total,
        "signal_resolved_total": signal_resolved_total,
        "signal_deduped_total": signal_deduped_total,
        "telegram_sent_total": telegram_sent_total,
        "telegram_failed_total": telegram_failed_total,
        "telegram_send_success_rate": telegram_send_success_rate,
    }


def compute_signal_metrics(records: list[SignalRecord]) -> dict:
    total = len(records)
    if total == 0:
        return {
            "signals_total": 0,
            "validated_total": 0,
            "invalidated_total": 0,
            "expired_total": 0,
            "validation_rate": 0.0,
            "invalidation_rate": 0.0,
            "expiry_rate": 0.0,
            "avg_mfe_pct": 0.0,
            "avg_mae_pct": 0.0,
            "avg_lifetime_min": 0.0,
            "avg_confidence": 0.0,
            "executable_signals": 0,
            "executable_rate": 0.0,
            "avg_rr": 0.0,
        }

    validated = [r for r in records if r.final_status == "VALIDATED"]
    invalidated = [r for r in records if r.final_status == "INVALIDATED"]
    expired = [r for r in records if r.final_status == "EXPIRED"]
    executable = [r for r in records if r.execution_status == "EXECUTABLE"]
    rr_values = [r.risk_reward_ratio for r in records if r.risk_reward_ratio is not None]

    avg_mfe = round(sum(r.mfe_pct for r in records) / total, 6)
    avg_mae = round(sum(r.mae_pct for r in records) / total, 6)
    avg_lifetime = round(sum(r.minutes_alive for r in records) / total, 2)
    avg_confidence = round(sum(r.confidence for r in records) / total, 6)
    avg_rr = round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0

    return {
        "signals_total": total,
        "validated_total": len(validated),
        "invalidated_total": len(invalidated),
        "expired_total": len(expired),
        "validation_rate": round(len(validated) / total, 4),
        "invalidation_rate": round(len(invalidated) / total, 4),
        "expiry_rate": round(len(expired) / total, 4),
        "avg_mfe_pct": avg_mfe,
        "avg_mae_pct": avg_mae,
        "avg_lifetime_min": avg_lifetime,
        "avg_confidence": avg_confidence,
        "executable_signals": len(executable),
        "executable_rate": round(len(executable) / total, 4),
        "avg_rr": avg_rr,
    }


def compute_metrics_by_symbol(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        grouped.setdefault(rec.symbol, []).append(rec)

    out: dict[str, dict] = {}
    for symbol, items in grouped.items():
        total = len(items)
        validated = sum(1 for r in items if r.final_status == "VALIDATED")
        invalidated = sum(1 for r in items if r.final_status == "INVALIDATED")
        expired = sum(1 for r in items if r.final_status == "EXPIRED")
        executable = sum(1 for r in items if r.execution_status == "EXECUTABLE")
        rr_values = [r.risk_reward_ratio for r in items if r.risk_reward_ratio is not None]

        out[symbol] = {
            "signals_total": total,
            "validated_total": validated,
            "invalidated_total": invalidated,
            "expired_total": expired,
            "validation_rate": round(validated / total, 4) if total else 0.0,
            "avg_mfe_pct": round(sum(r.mfe_pct for r in items) / total, 6) if total else 0.0,
            "avg_mae_pct": round(sum(r.mae_pct for r in items) / total, 6) if total else 0.0,
            "avg_confidence": round(sum(r.confidence for r in items) / total, 6) if total else 0.0,
            "executable_signals": executable,
            "executable_rate": round(executable / total, 4) if total else 0.0,
            "avg_rr": round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0,
        }
    return out


def compute_metrics_by_scenario(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        grouped.setdefault(rec.scenario, []).append(rec)

    out: dict[str, dict] = {}
    for scenario, items in grouped.items():
        total = len(items)
        validated = sum(1 for r in items if r.final_status == "VALIDATED")
        invalidated = sum(1 for r in items if r.final_status == "INVALIDATED")
        expired = sum(1 for r in items if r.final_status == "EXPIRED")
        executable = sum(1 for r in items if r.execution_status == "EXECUTABLE")
        rr_values = [r.risk_reward_ratio for r in items if r.risk_reward_ratio is not None]

        out[scenario] = {
            "signals_total": total,
            "validated_total": validated,
            "invalidated_total": invalidated,
            "expired_total": expired,
            "validation_rate": round(validated / total, 4) if total else 0.0,
            "avg_mfe_pct": round(sum(r.mfe_pct for r in items) / total, 6) if total else 0.0,
            "avg_mae_pct": round(sum(r.mae_pct for r in items) / total, 6) if total else 0.0,
            "avg_confidence": round(sum(r.confidence for r in items) / total, 6) if total else 0.0,
            "executable_signals": executable,
            "executable_rate": round(executable / total, 4) if total else 0.0,
            "avg_rr": round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0,
        }
    return out


def compute_confidence_buckets(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        bucket = _confidence_bucket(rec.confidence)
        grouped.setdefault(bucket, []).append(rec)

    out: dict[str, dict] = {}
    for bucket, items in grouped.items():
        total = len(items)
        validated = sum(1 for r in items if r.final_status == "VALIDATED")
        invalidated = sum(1 for r in items if r.final_status == "INVALIDATED")
        expired = sum(1 for r in items if r.final_status == "EXPIRED")
        executable = sum(1 for r in items if r.execution_status == "EXECUTABLE")
        rr_values = [r.risk_reward_ratio for r in items if r.risk_reward_ratio is not None]

        out[bucket] = {
            "signals_total": total,
            "validated_total": validated,
            "invalidated_total": invalidated,
            "expired_total": expired,
            "validation_rate": round(validated / total, 4) if total else 0.0,
            "avg_mfe_pct": round(sum(r.mfe_pct for r in items) / total, 6) if total else 0.0,
            "avg_mae_pct": round(sum(r.mae_pct for r in items) / total, 6) if total else 0.0,
            "executable_signals": executable,
            "executable_rate": round(executable / total, 4) if total else 0.0,
            "avg_rr": round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0,
        }
    return out


def compute_metrics_by_execution_model(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        key = rec.execution_model or "UNKNOWN"
        grouped.setdefault(key, []).append(rec)

    out: dict[str, dict] = {}
    for model, items in grouped.items():
        total = len(items)
        validated = sum(1 for r in items if r.final_status == "VALIDATED")
        invalidated = sum(1 for r in items if r.final_status == "INVALIDATED")
        expired = sum(1 for r in items if r.final_status == "EXPIRED")
        rr_values = [r.risk_reward_ratio for r in items if r.risk_reward_ratio is not None]

        out[model] = {
            "signals_total": total,
            "validated_total": validated,
            "invalidated_total": invalidated,
            "expired_total": expired,
            "validation_rate": round(validated / total, 4) if total else 0.0,
            "avg_confidence": round(sum(r.confidence for r in items) / total, 6) if total else 0.0,
            "avg_rr": round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0,
        }
    return out


def records_to_dataframe(records: list[SignalRecord]) -> pd.DataFrame:
    rows = [r.to_dict() for r in records]
    if not rows:
        return pd.DataFrame(columns=[
            "signal_id",
            "symbol",
            "timeframe",
            "cycle_id",
            "created_at_utc",
            "scenario",
            "signal_class",
            "direction",
            "market_state",
            "htf_bias",
            "confidence",
            "entry_reference_price",
            "invalidation_reference_price",
            "target_reference_price",
            "execution_status",
            "execution_model",
            "risk_reward_ratio",
            "stop_distance",
            "target_distance",
            "execution_timeframe",
            "trigger_reason",
            "was_sent_to_telegram",
            "was_deduped",
            "current_stage",
            "final_status",
            "resolution_reason",
            "bars_alive",
            "minutes_alive",
            "mfe_pct",
            "mae_pct",
            "time_to_validation_min",
            "runner_version",
        ])
    return pd.DataFrame(rows)


def export_signal_records_json(
    records: list[SignalRecord],
    path: Path | str = DEFAULT_SIGNAL_RECORDS_JSON_PATH,
) -> None:
    path = Path(path)
    _ensure_parent_dir(path)
    payload = [r.to_dict() for r in records]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def export_signal_records_parquet(
    records: list[SignalRecord],
    path: Path | str = DEFAULT_SIGNAL_RECORDS_PARQUET_PATH,
) -> None:
    path = Path(path)
    _ensure_parent_dir(path)
    df = records_to_dataframe(records)
    df.to_parquet(path, index=False)


def export_daily_summary(
    summary: dict,
    path: Path | str = DEFAULT_DAILY_SUMMARY_PATH,
) -> None:
    path = Path(path)
    _ensure_parent_dir(path)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def build_statistics_bundle(
    journal_path: Path | str = DEFAULT_JOURNAL_PATH,
) -> dict:
    events = load_journal_events(journal_path)
    records = build_signal_records(events)

    bundle = {
        "system_metrics": compute_system_metrics(events),
        "signal_metrics": compute_signal_metrics(records),
        "metrics_by_symbol": compute_metrics_by_symbol(records),
        "metrics_by_scenario": compute_metrics_by_scenario(records),
        "confidence_buckets": compute_confidence_buckets(records),
        "metrics_by_execution_model": compute_metrics_by_execution_model(records),
        "records_count": len(records),
        "events_count": len(events),
    }
    return bundle


def build_and_export_statistics(
    journal_path: Path | str = DEFAULT_JOURNAL_PATH,
    records_json_path: Path | str = DEFAULT_SIGNAL_RECORDS_JSON_PATH,
    records_parquet_path: Path | str = DEFAULT_SIGNAL_RECORDS_PARQUET_PATH,
    daily_summary_path: Path | str = DEFAULT_DAILY_SUMMARY_PATH,
) -> dict:
    events = load_journal_events(journal_path)
    records = build_signal_records(events)
    bundle = {
        "system_metrics": compute_system_metrics(events),
        "signal_metrics": compute_signal_metrics(records),
        "metrics_by_symbol": compute_metrics_by_symbol(records),
        "metrics_by_scenario": compute_metrics_by_scenario(records),
        "confidence_buckets": compute_confidence_buckets(records),
        "metrics_by_execution_model": compute_metrics_by_execution_model(records),
        "records_count": len(records),
        "events_count": len(events),
    }

    export_signal_records_json(records, records_json_path)
    export_signal_records_parquet(records, records_parquet_path)
    export_daily_summary(bundle, daily_summary_path)

    return bundle


def main() -> None:
    bundle = build_and_export_statistics()
    print(json.dumps(bundle, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()