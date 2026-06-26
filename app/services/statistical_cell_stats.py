from __future__ import annotations

"""
Statistical Cell Stats builder for AI Market Analyst.

Version: statistical-cell-stats-v1.1-dedup-evaluated-cells

Purpose
-------
Build per-cell outcome statistics consumed by statistical_permission_gate.py.

The cell contract follows the second research implementation direction:
    setup × instrument × session × regime/day-type × direction

This module is deliberately storage-facing and dependency-light:
- reads runtime/stats/signal_outcomes.json;
- extracts TP_HIT / SL_HIT closed observations;
- aggregates wins, losses, avg_win_r, avg_loss_r, total_net_r,
  last_20_net_expectancy_r;
- writes runtime/stats/statistical_cell_stats.json;
- does not change Telegram/Battle decisions.

It is safe to run as a scheduled/export job or import from Battle Gate in
shadow mode.
"""

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Mapping

from app.core.settings import settings
from app.services.statistical_permission_gate import (
    STATISTICAL_PERMISSION_GATE_VERSION,
    build_statistical_cell_key,
    evaluate_statistical_permission,
)


STATISTICAL_CELL_STATS_VERSION = "statistical-cell-stats-v1.1-dedup-evaluated-cells"

STATS_DIR = settings.runtime_dir / "stats"
SIGNAL_OUTCOMES_PATH = STATS_DIR / "signal_outcomes.json"
STATISTICAL_CELL_STATS_PATH = STATS_DIR / "statistical_cell_stats.json"

TP_STATUS = "TP_HIT"
SL_STATUS = "SL_HIT"
CLOSED_TP_SL_STATUSES = {TP_STATUS, SL_STATUS}
FINAL_NON_TP_SL_STATUSES = {
    "EXPIRED",
    "EXPIRED_AFTER_ENTRY",
    "MISSED_TARGET_BEFORE_ENTRY",
    "INVALID",
}
SYNTHETIC_SIGNAL_PREFIXES = ("TEST_", "SYNTHETIC_")
SYNTHETIC_CYCLE_IDS = {"SYNTHETIC_TEST", "TEST"}
SYNTHETIC_TRACKING_SCOPE = "SYNTHETIC_TEST"


@dataclass
class StatisticalCellAccumulator:
    cell_key: str
    wins: int = 0
    losses: int = 0
    total_signals: int = 0
    total_final: int = 0
    expired: int = 0
    missed: int = 0
    invalid: int = 0
    near_target: int = 0
    pending: int = 0
    synthetic_excluded: int = 0
    outcome_status: Counter[str] = field(default_factory=Counter)
    tracking_scope: Counter[str] = field(default_factory=Counter)
    battle_permission: Counter[str] = field(default_factory=Counter)
    telegram_delivery_mode: Counter[str] = field(default_factory=Counter)
    symbols: Counter[str] = field(default_factory=Counter)
    scenarios: Counter[str] = field(default_factory=Counter)
    sessions: Counter[str] = field(default_factory=Counter)
    regimes: Counter[str] = field(default_factory=Counter)
    day_types: Counter[str] = field(default_factory=Counter)
    directions: Counter[str] = field(default_factory=Counter)
    win_r_values: list[float] = field(default_factory=list)
    loss_r_values: list[float] = field(default_factory=list)
    net_r_values: list[float] = field(default_factory=list)
    closed_trades: list[dict[str, Any]] = field(default_factory=list)

    @property
    def closed_trades_count(self) -> int:
        return int(self.wins) + int(self.losses)

    def to_gate_stats(self) -> dict[str, Any]:
        avg_win_r = mean(self.win_r_values) if self.win_r_values else None
        avg_loss_r = mean(self.loss_r_values) if self.loss_r_values else None
        total_net_r = sum(self.net_r_values) if self.net_r_values else None
        total_gross_r = total_net_r
        last_20 = self.closed_trades[-20:]
        last_20_r = [safe_float(x.get("result_R")) for x in last_20]
        last_20_r = [x for x in last_20_r if x is not None]
        last_20_net_expectancy = mean(last_20_r) if last_20_r else None

        return {
            "cell_key": self.cell_key,
            "wins": self.wins,
            "losses": self.losses,
            "closed_trades_count": self.closed_trades_count,
            "avg_win_r": round(avg_win_r, 6) if avg_win_r is not None else None,
            "avg_loss_r": round(avg_loss_r, 6) if avg_loss_r is not None else None,
            "total_gross_r": round(total_gross_r, 6) if total_gross_r is not None else None,
            "total_net_r": round(total_net_r, 6) if total_net_r is not None else None,
            "last_20_net_expectancy_r": (
                round(last_20_net_expectancy, 6) if last_20_net_expectancy is not None else None
            ),
            "closed_trades": self.closed_trades,
            "total_signals": self.total_signals,
            "total_final": self.total_final,
            "expired": self.expired,
            "missed": self.missed,
            "invalid": self.invalid,
            "near_target": self.near_target,
            "pending": self.pending,
            "outcome_status": dict(self.outcome_status),
            "tracking_scope": dict(self.tracking_scope),
            "battle_permission": dict(self.battle_permission),
            "telegram_delivery_mode": dict(self.telegram_delivery_mode),
            "top_symbols": dict(self.symbols.most_common(10)),
            "top_scenarios": dict(self.scenarios.most_common(10)),
            "top_sessions": dict(self.sessions.most_common(10)),
            "top_regimes": dict(self.regimes.most_common(10)),
            "top_day_types": dict(self.day_types.most_common(10)),
            "top_directions": dict(self.directions.most_common(10)),
        }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        f = float(value)
        if f != f or f in {float("inf"), float("-inf")}:
            return default
        return f
    except Exception:
        return default


def normalize_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def normalize_status(value: Any) -> str:
    return normalize_text(value).upper()


def first_non_empty(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value in (None, "", [], {}):
            continue
        return value
    return default


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def is_synthetic_record(record: Mapping[str, Any]) -> bool:
    signal_id = normalize_text(record.get("signal_id")).upper()
    cycle_id = normalize_text(record.get("cycle_id")).upper()
    tracking_scope = normalize_text(record.get("tracking_scope")).upper()

    if tracking_scope == SYNTHETIC_TRACKING_SCOPE:
        return True
    if cycle_id in SYNTHETIC_CYCLE_IDS:
        return True
    if any(signal_id.startswith(prefix) for prefix in SYNTHETIC_SIGNAL_PREFIXES):
        return True
    return False


def _looks_like_signal_record(item: Mapping[str, Any]) -> bool:
    if "outcome_status" in item or "signal_id" in item:
        return True
    if "symbol" in item and any(k in item for k in ("scenario", "direction", "result_R")):
        return True
    return False


def extract_signal_records(raw: Any) -> list[dict[str, Any]]:
    """Extract flat signal records from common signal_outcomes.json shapes.

    Supported shapes:
    - list[dict]
    - {"signals": [...]}, {"records": [...]}, {"items": [...]}, {"outcomes": [...]}
    - nested bucket dicts containing lists of records.

    Summary-only dicts are ignored unless they look like individual signal records.
    """
    records: list[dict[str, Any]] = []
    seen_ids: set[int] = set()

    def walk(value: Any, depth: int = 0) -> None:
        if depth > 8:
            return
        if isinstance(value, list):
            for child in value:
                walk(child, depth + 1)
            return
        if not isinstance(value, dict):
            return

        obj_id = id(value)
        if obj_id in seen_ids:
            return
        seen_ids.add(obj_id)

        if _looks_like_signal_record(value):
            records.append(dict(value))
            return

        preferred_keys = (
            "signals",
            "records",
            "items",
            "outcomes",
            "data",
            "production",
            "research",
            "alerts",
            "all",
        )
        for key in preferred_keys:
            child = value.get(key)
            if isinstance(child, (list, dict)):
                walk(child, depth + 1)

        # Fallback for bucketed dicts. Avoid walking obvious scalar summaries too deeply.
        for child in value.values():
            if isinstance(child, (list, dict)):
                walk(child, depth + 1)

    walk(raw)
    return records


def _record_sort_timestamp(record: Mapping[str, Any]) -> str:
    return str(
        first_non_empty(
            record.get("closed_at_utc"),
            record.get("updated_at_utc"),
            record.get("last_seen_at_utc"),
            record.get("created_at_utc"),
            record.get("ts_utc"),
            record.get("ts"),
            default="",
        )
    )


def _positive_r_for_win(record: Mapping[str, Any]) -> float:
    explicit = safe_float(
        first_non_empty(
            record.get("result_R"),
            record.get("outcome_R"),
            record.get("net_r"),
            record.get("r"),
        )
    )
    if explicit is not None and explicit > 0:
        return explicit

    rr = safe_float(
        first_non_empty(
            record.get("practical_rr"),
            record.get("risk_reward_ratio"),
            record.get("rr"),
            record.get("planned_rr"),
        )
    )
    if rr is not None and rr > 0:
        return rr
    return 1.0


def _positive_r_for_loss(record: Mapping[str, Any]) -> float:
    explicit = safe_float(
        first_non_empty(
            record.get("result_R"),
            record.get("outcome_R"),
            record.get("net_r"),
            record.get("r"),
        )
    )
    if explicit is not None and explicit < 0:
        return abs(explicit)
    return 1.0


def _payload_for_cell_key(record: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(record)
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    payload["metadata"] = metadata

    # Some outcome records use tracking_scope as the most stable session-like
    # distinction. Do not overwrite explicit session fields when present.
    payload.setdefault("session", first_non_empty(record.get("session"), record.get("batch_group"), record.get("tracking_scope")))
    payload.setdefault("regime", first_non_empty(record.get("regime"), record.get("market_regime"), record.get("day_type"), record.get("day_type_candidate")))
    return payload


def _record_deduplication_key(record: Mapping[str, Any]) -> str:
    """Return a stable key for one trade/counterfactual observation.

    signal_outcomes.json can contain repeated research counterfactual records
    created from repeated telemetry evaluations of the same signal. For
    statistical permission gating, one signal_id must count as one observation
    inside its cell, otherwise repeated blocked telemetry can artificially
    inflate the sample and distort posterior/expectancy.
    """
    payload = _payload_for_cell_key(record)
    cell_key = build_statistical_cell_key(payload)
    signal_id = normalize_text(record.get("signal_id"))
    tracking_scope = normalize_text(
        first_non_empty(record.get("tracking_scope"), record.get("scope"), default="UNKNOWN"),
        "UNKNOWN",
    )

    if signal_id:
        return f"{cell_key}|{tracking_scope}|{signal_id}"

    # Fallback for older/manual rows without signal_id: only exact duplicates
    # collapse. This avoids accidentally merging unrelated unknown rows.
    outcome = normalize_status(first_non_empty(record.get("outcome_status"), record.get("status")))
    closed_at = normalize_text(first_non_empty(record.get("closed_at_utc"), record.get("updated_at_utc"), record.get("ts_utc")))
    result_r = normalize_text(first_non_empty(record.get("result_R"), record.get("outcome_R"), record.get("net_r")))
    symbol = normalize_text(first_non_empty(record.get("symbol"), record.get("instrument")), "UNKNOWN")
    direction = normalize_text(first_non_empty(record.get("direction"), record.get("side")), "UNKNOWN")
    return f"{cell_key}|NO_SIGNAL_ID|{symbol}|{direction}|{tracking_scope}|{outcome}|{closed_at}|{result_r}"


def _deduplicate_records_for_cells(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Keep the latest record per statistical cell + signal_id + tracking scope."""
    deduped: dict[str, dict[str, Any]] = {}
    skipped = 0

    for record in sorted(records, key=_record_sort_timestamp):
        key = _record_deduplication_key(record)
        if key in deduped:
            skipped += 1
        # Input is sorted ascending, so later/final material state wins.
        deduped[key] = record

    return list(deduped.values()), skipped


def _counter_add(counter: Counter[str], value: Any, default: str = "UNKNOWN") -> None:
    text = normalize_text(value, default)
    counter[text] += 1


def build_cell_stats_from_records(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    cells: dict[str, StatisticalCellAccumulator] = {}
    total_records = 0
    synthetic_excluded = 0
    closed_tp_sl = 0

    sorted_records = sorted(
        [dict(r) for r in records if isinstance(r, Mapping)],
        key=_record_sort_timestamp,
    )

    non_synthetic_records: list[dict[str, Any]] = []
    for record in sorted_records:
        total_records += 1
        if is_synthetic_record(record):
            synthetic_excluded += 1
            continue
        non_synthetic_records.append(record)

    records_for_stats, deduplicated_records_skipped = _deduplicate_records_for_cells(
        non_synthetic_records
    )

    for record in records_for_stats:
        payload = _payload_for_cell_key(record)
        cell_key = build_statistical_cell_key(payload)
        acc = cells.setdefault(cell_key, StatisticalCellAccumulator(cell_key=cell_key))
        acc.total_signals += 1

        outcome = normalize_status(first_non_empty(record.get("outcome_status"), record.get("status")))
        tracking_scope = first_non_empty(record.get("tracking_scope"), record.get("scope"), default="UNKNOWN")
        battle_permission = first_non_empty(record.get("battle_permission"), default="UNKNOWN")
        delivery_mode = first_non_empty(record.get("telegram_delivery_mode"), default="UNKNOWN")

        acc.outcome_status[outcome or "UNKNOWN"] += 1
        _counter_add(acc.tracking_scope, tracking_scope)
        _counter_add(acc.battle_permission, battle_permission)
        _counter_add(acc.telegram_delivery_mode, delivery_mode)
        _counter_add(acc.symbols, first_non_empty(record.get("symbol"), record.get("instrument")))
        _counter_add(acc.scenarios, first_non_empty(record.get("scenario"), record.get("scenario_type"), record.get("setup_type")))
        _counter_add(acc.sessions, first_non_empty(record.get("session"), record.get("batch_group"), tracking_scope))
        _counter_add(acc.regimes, first_non_empty(record.get("regime"), record.get("market_regime"), record.get("macro_regime"), record.get("day_type"), record.get("day_type_candidate")))
        _counter_add(acc.day_types, first_non_empty(record.get("day_type"), record.get("day_type_candidate"), record.get("auction_day_type")))
        _counter_add(acc.directions, first_non_empty(record.get("direction"), record.get("side")))

        if outcome in CLOSED_TP_SL_STATUSES or outcome in FINAL_NON_TP_SL_STATUSES:
            acc.total_final += 1

        if outcome == TP_STATUS:
            r_value = _positive_r_for_win(record)
            acc.wins += 1
            acc.win_r_values.append(r_value)
            acc.net_r_values.append(r_value)
            acc.closed_trades.append(
                {
                    "signal_id": record.get("signal_id"),
                    "outcome_status": outcome,
                    "result_R": round(r_value, 6),
                    "closed_at_utc": record.get("closed_at_utc"),
                    "tracking_scope": tracking_scope,
                    "battle_permission": battle_permission,
                    "telegram_delivery_mode": delivery_mode,
                }
            )
            closed_tp_sl += 1
        elif outcome == SL_STATUS:
            loss_r = _positive_r_for_loss(record)
            acc.losses += 1
            acc.loss_r_values.append(loss_r)
            acc.net_r_values.append(-loss_r)
            acc.closed_trades.append(
                {
                    "signal_id": record.get("signal_id"),
                    "outcome_status": outcome,
                    "result_R": round(-loss_r, 6),
                    "closed_at_utc": record.get("closed_at_utc"),
                    "tracking_scope": tracking_scope,
                    "battle_permission": battle_permission,
                    "telegram_delivery_mode": delivery_mode,
                }
            )
            closed_tp_sl += 1
        elif outcome in {"EXPIRED", "EXPIRED_AFTER_ENTRY"}:
            acc.expired += 1
        elif outcome == "MISSED_TARGET_BEFORE_ENTRY":
            acc.missed += 1
        elif outcome == "INVALID":
            acc.invalid += 1
        elif outcome in {"NEAR_TARGET_REACHED", "NEAR_TP", "OPEN_NEAR_TARGET"}:
            acc.near_target += 1
        else:
            acc.pending += 1

    cell_payloads = {key: acc.to_gate_stats() for key, acc in sorted(cells.items())}

    # Persist the shadow statistical gate decision per cell. The CLI already
    # displays these values, but live lookup/debugging should not require a
    # second ad-hoc evaluation step.
    for key, cell in cell_payloads.items():
        result = evaluate_statistical_permission({}, cell, cell_key=key).to_dict()
        for result_key in (
            "evidence_tier",
            "posterior_alpha",
            "posterior_beta",
            "posterior_mean",
            "posterior_lower_95",
            "posterior_upper_95",
            "raw_winrate",
            "avg_win_r",
            "avg_loss_r",
            "gross_expectancy_r",
            "net_expectancy_r",
            "statistical_permission",
            "statistical_status",
            "statistical_multiplier",
            "allows_ready",
            "blockers",
            "reasons",
            "modifiers",
        ):
            cell[f"stat_{result_key}"] = result.get(result_key)

    return {
        "schema_version": "1.1",
        "version": STATISTICAL_CELL_STATS_VERSION,
        "statistical_permission_gate_version": STATISTICAL_PERMISSION_GATE_VERSION,
        "generated_at_utc": utc_now_iso(),
        "source": "signal_outcomes.json",
        "total_records_seen": total_records,
        "synthetic_excluded": synthetic_excluded,
        "raw_production_records_seen": total_records - synthetic_excluded,
        "deduplicated_records_skipped": deduplicated_records_skipped,
        "production_records_used": len(records_for_stats),
        "closed_tp_sl_used": closed_tp_sl,
        "cell_count": len(cell_payloads),
        "cells": cell_payloads,
    }


def build_cell_stats_from_signal_outcomes(
    source_path: Path = SIGNAL_OUTCOMES_PATH,
    output_path: Path | None = STATISTICAL_CELL_STATS_PATH,
) -> dict[str, Any]:
    raw = read_json(source_path, {})
    records = extract_signal_records(raw)
    payload = build_cell_stats_from_records(records)
    payload["source_path"] = str(source_path)
    if output_path is not None:
        write_json(output_path, payload)
        payload["output_path"] = str(output_path)
    return payload


def load_statistical_cell_stats(path: Path = STATISTICAL_CELL_STATS_PATH) -> dict[str, Any]:
    raw = read_json(path, {})
    if not isinstance(raw, dict):
        return {"cells": {}}
    cells = raw.get("cells")
    if not isinstance(cells, dict):
        raw["cells"] = {}
    return raw


def get_cell_stats_for_payload(
    payload: Mapping[str, Any] | None,
    stats_payload: Mapping[str, Any] | None = None,
    *,
    stats_path: Path = STATISTICAL_CELL_STATS_PATH,
) -> dict[str, Any] | None:
    stats_payload = stats_payload if isinstance(stats_payload, Mapping) else load_statistical_cell_stats(stats_path)
    cells = stats_payload.get("cells") if isinstance(stats_payload, Mapping) else None
    if not isinstance(cells, Mapping):
        return None
    key = build_statistical_cell_key(payload or {})
    cell = cells.get(key)
    return dict(cell) if isinstance(cell, Mapping) else None


def summarize_top_cells(stats_payload: Mapping[str, Any], *, limit: int = 20) -> list[dict[str, Any]]:
    cells = stats_payload.get("cells") if isinstance(stats_payload, Mapping) else None
    if not isinstance(cells, Mapping):
        return []

    rows: list[dict[str, Any]] = []
    for key, value in cells.items():
        if not isinstance(value, Mapping):
            continue
        wins = int(value.get("wins") or 0)
        losses = int(value.get("losses") or 0)
        n = wins + losses
        winrate = (wins / n) if n else None
        rows.append(
            {
                "cell_key": key,
                "closed_trades": n,
                "wins": wins,
                "losses": losses,
                "raw_winrate": round(winrate, 4) if winrate is not None else None,
                "avg_win_r": value.get("avg_win_r"),
                "avg_loss_r": value.get("avg_loss_r"),
                "total_net_r": value.get("total_net_r"),
                "last_20_net_expectancy_r": value.get("last_20_net_expectancy_r"),
            }
        )

    rows.sort(key=lambda x: (int(x.get("closed_trades") or 0), float(x.get("total_net_r") or 0.0)), reverse=True)
    return rows[:limit]


def evaluate_top_cells(stats_payload: Mapping[str, Any], *, limit: int = 20) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in summarize_top_cells(stats_payload, limit=limit):
        cell_key = row["cell_key"]
        cell = stats_payload.get("cells", {}).get(cell_key, {}) if isinstance(stats_payload.get("cells"), Mapping) else {}
        result = evaluate_statistical_permission({}, cell, cell_key=cell_key).to_dict()
        out.append({**row, **{f"stat_{k}": v for k, v in result.items() if k in {
            "evidence_tier",
            "posterior_lower_95",
            "net_expectancy_r",
            "statistical_permission",
            "statistical_status",
            "allows_ready",
            "blockers",
        }}})
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build statistical cell stats from signal_outcomes.json.")
    parser.add_argument("--source", default=str(SIGNAL_OUTCOMES_PATH), help="Path to signal_outcomes.json")
    parser.add_argument("--output", default=str(STATISTICAL_CELL_STATS_PATH), help="Output statistical_cell_stats.json path")
    parser.add_argument("--top", type=int, default=20, help="Print top N cells")
    args = parser.parse_args()

    source_path = Path(args.source)
    output_path = Path(args.output) if args.output else None
    payload = build_cell_stats_from_signal_outcomes(source_path=source_path, output_path=output_path)

    print(json.dumps({
        "version": payload.get("version"),
        "source_path": payload.get("source_path"),
        "output_path": payload.get("output_path"),
        "total_records_seen": payload.get("total_records_seen"),
        "synthetic_excluded": payload.get("synthetic_excluded"),
        "production_records_used": payload.get("production_records_used"),
        "closed_tp_sl_used": payload.get("closed_tp_sl_used"),
        "cell_count": payload.get("cell_count"),
    }, ensure_ascii=False, indent=2))

    top = evaluate_top_cells(payload, limit=max(0, int(args.top or 0)))
    if top:
        print("\nTOP CELLS")
        for row in top:
            print(json.dumps(row, ensure_ascii=False))


if __name__ == "__main__":
    main()
