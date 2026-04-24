from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ======================================================================================
# PATHS / CONSTANTS
# ======================================================================================

DEFAULT_RUNTIME_DIR = Path("/var/data/runtime")
LOCAL_RUNTIME_DIR = Path("runtime")

DEFAULT_OUTPUT_DIR = Path("/var/data/runtime/edge_dashboard")
LOCAL_OUTPUT_DIR = Path("runtime/edge_dashboard")

SNAPSHOT_FILENAME = "radar_snapshot_v2.ndjson"
JOURNAL_FILENAME = "radar_journal.ndjson"

SUMMARY_JSON_FILENAME = "edge_summary.json"
SCENARIO_EDGE_SUMMARY_JSON_FILENAME = "scenario_edge_summary.json"
TIME_EDGE_SUMMARY_JSON_FILENAME = "time_edge_summary.json"

CSV_MARKET_REGIME = "market_regime_distribution.csv"
CSV_SCENARIO_FREQUENCY = "scenario_frequency.csv"
CSV_OPPORTUNITY_RATE = "opportunity_rate.csv"
CSV_EXECUTION_COMPLETENESS = "execution_completeness.csv"
CSV_CONSISTENCY = "consistency_score.csv"
CSV_SIGNAL_LIFECYCLE = "signal_lifecycle_stats.csv"
CSV_SYMBOL_OVERVIEW = "symbol_overview.csv"
CSV_DAILY_OVERVIEW = "daily_overview.csv"
CSV_SCENARIO_EDGE_MATRIX = "scenario_edge_matrix.csv"

CSV_TIME_EDGE_BY_HOUR = "time_edge_by_hour.csv"
CSV_TIME_EDGE_BY_WEEKDAY = "time_edge_by_weekday.csv"
CSV_TIME_EDGE_BY_SESSION = "time_edge_by_session.csv"


# ======================================================================================
# HELPERS
# ======================================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_str(value: Any, default: str = "UNKNOWN") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def parse_any_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def to_day_key(dt: Optional[datetime]) -> str:
    if dt is None:
        return "UNKNOWN_DATE"
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")


def safe_read_ndjson(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows

    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    rows.append(item)
            except json.JSONDecodeError:
                continue
    return rows


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def avg(values: Iterable[float]) -> float:
    vals = [v for v in values if v is not None]
    if not vals:
        return 0.0
    return round(sum(vals) / len(vals), 4)


def get_first(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return default


def coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        x = value.strip().lower()
        if x in {"true", "1", "yes", "y"}:
            return True
        if x in {"false", "0", "no", "n"}:
            return False
    return None


def dt_hour_utc(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).hour


def dt_weekday_utc(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).weekday()  # Monday=0


def weekday_name(index: Optional[int]) -> str:
    names = [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY",
        "SATURDAY",
        "SUNDAY",
    ]
    if index is None or index < 0 or index > 6:
        return "UNKNOWN"
    return names[index]


def detect_session_utc(dt: Optional[datetime]) -> str:
    hour = dt_hour_utc(dt)
    if hour is None:
        return "UNKNOWN"
    if 0 <= hour <= 7:
        return "ASIA"
    if 8 <= hour <= 12:
        return "LONDON"
    if 13 <= hour <= 21:
        return "NEW_YORK"
    return "LATE_US"


# ======================================================================================
# DATA MODELS
# ======================================================================================

@dataclass
class SnapshotRecord:
    timestamp: Optional[datetime]
    day: str
    symbol: str
    market_state: str
    htf_bias: str
    scenario: str
    signal_class: str
    direction: str
    opportunity_flag: bool
    execution_ready_flag: bool
    probability: Optional[float]
    consistency_score: Optional[float]
    raw: Dict[str, Any]


@dataclass
class JournalRecord:
    timestamp: Optional[datetime]
    day: str
    symbol: str
    event_type: str
    signal_id: str
    scenario: str
    signal_class: str
    direction: str
    market_state: str
    htf_bias: str
    status: str
    alert_type: str
    consistency_score: Optional[float]
    raw: Dict[str, Any]


# ======================================================================================
# NORMALIZATION
# ======================================================================================

class EdgeDashboardNormalizer:
    @staticmethod
    def normalize_snapshot(row: Dict[str, Any]) -> SnapshotRecord:
        ts = parse_any_datetime(
            get_first(
                row,
                "timestamp",
                "created_at_utc",
                "updated_at_utc",
                "cycle_id",
                "generated_at",
            )
        )

        symbol = normalize_str(get_first(row, "symbol", "instrument", default="UNKNOWN"))
        market_state = normalize_str(get_first(row, "market_state", "regime", default="UNKNOWN"))
        htf_bias = normalize_str(get_first(row, "htf_bias", "bias", default="UNKNOWN"))
        scenario = normalize_str(get_first(row, "scenario", "scenario_name", default="UNKNOWN"))
        signal_class = normalize_str(get_first(row, "signal_class", "classification", default="UNKNOWN"))
        direction = normalize_str(get_first(row, "direction", default="UNKNOWN"))

        probability = safe_float(get_first(row, "probability", "score", "confidence"))
        consistency_score = safe_float(
            get_first(row, "consistency_score", "consistency", "setup_consistency")
        )

        opportunity_flag = EdgeDashboardNormalizer._detect_opportunity_flag(row)
        execution_ready_flag = EdgeDashboardNormalizer._detect_execution_ready_flag(row)

        return SnapshotRecord(
            timestamp=ts,
            day=to_day_key(ts),
            symbol=symbol,
            market_state=market_state,
            htf_bias=htf_bias,
            scenario=scenario,
            signal_class=signal_class,
            direction=direction,
            opportunity_flag=opportunity_flag,
            execution_ready_flag=execution_ready_flag,
            probability=probability,
            consistency_score=consistency_score,
            raw=row,
        )

    @staticmethod
    def normalize_journal(row: Dict[str, Any]) -> JournalRecord:
        ts = parse_any_datetime(
            get_first(
                row,
                "timestamp",
                "created_at_utc",
                "updated_at_utc",
                "event_at",
                "cycle_id",
            )
        )

        symbol = normalize_str(get_first(row, "symbol", "instrument", default="UNKNOWN"))
        signal_id = normalize_str(get_first(row, "signal_id", default="UNKNOWN_SIGNAL"))
        scenario = normalize_str(get_first(row, "scenario", "scenario_name", default="UNKNOWN"))
        signal_class = normalize_str(get_first(row, "signal_class", "classification", default="UNKNOWN"))
        direction = normalize_str(get_first(row, "direction", default="UNKNOWN"))
        market_state = normalize_str(get_first(row, "market_state", "regime", default="UNKNOWN"))
        htf_bias = normalize_str(get_first(row, "htf_bias", "bias", default="UNKNOWN"))

        event_type = normalize_str(
            get_first(row, "event_type", "event", "lifecycle_event", "journal_event", default="UNKNOWN")
        )
        status = normalize_str(get_first(row, "status", "signal_status", default="UNKNOWN"))
        alert_type = normalize_str(get_first(row, "alert_type", "alert", "alert_event", default="UNKNOWN"))
        consistency_score = safe_float(
            get_first(row, "consistency_score", "consistency", "setup_consistency")
        )

        return JournalRecord(
            timestamp=ts,
            day=to_day_key(ts),
            symbol=symbol,
            event_type=event_type,
            signal_id=signal_id,
            scenario=scenario,
            signal_class=signal_class,
            direction=direction,
            market_state=market_state,
            htf_bias=htf_bias,
            status=status,
            alert_type=alert_type,
            consistency_score=consistency_score,
            raw=row,
        )

    @staticmethod
    def _detect_opportunity_flag(row: Dict[str, Any]) -> bool:
        direct = coerce_bool(get_first(row, "opportunity_flag", "is_opportunity"))
        if direct is not None:
            return direct

        signal_class = normalize_str(get_first(row, "signal_class", default="")).upper()
        alert_type = normalize_str(get_first(row, "alert_type", "alert", default="")).upper()
        probability = safe_float(get_first(row, "probability", "score", "confidence"))

        if signal_class in {"READY", "ACTIONABLE", "TRADE", "ENTRY", "SETUP"}:
            return True
        if alert_type in {"WATCH_NEW", "READY_NEW", "ENTRY_READY", "TRADE_READY"}:
            return True
        if probability is not None and probability >= 50:
            return True
        return False

    @staticmethod
    def _detect_execution_ready_flag(row: Dict[str, Any]) -> bool:
        direct = coerce_bool(get_first(row, "execution_ready_flag", "is_execution_ready"))
        if direct is not None:
            return direct

        signal_class = normalize_str(get_first(row, "signal_class", default="")).upper()
        status = normalize_str(get_first(row, "status", "signal_status", default="")).upper()
        alert_type = normalize_str(get_first(row, "alert_type", "alert", default="")).upper()

        markers = {"READY", "ENTRY", "TRADE", "ACTIONABLE", "EXECUTABLE"}
        if signal_class in markers:
            return True
        if status in {"READY", "OPEN", "ACTIVE", "TRIGGERED"}:
            return True
        if alert_type in {"ENTRY_READY", "TRADE_READY", "SIGNAL_TRIGGERED"}:
            return True
        return False


# ======================================================================================
# SERVICE
# ======================================================================================

class EdgeDashboardService:
    def __init__(
        self,
        runtime_dir: Optional[Path] = None,
        output_dir: Optional[Path] = None,
    ) -> None:
        self.runtime_dir = self._resolve_runtime_dir(runtime_dir)
        self.output_dir = self._resolve_output_dir(output_dir)

        self.snapshot_path = self.runtime_dir / SNAPSHOT_FILENAME
        self.journal_path = self.runtime_dir / JOURNAL_FILENAME

    @staticmethod
    def _resolve_runtime_dir(runtime_dir: Optional[Path]) -> Path:
        if runtime_dir is not None:
            return runtime_dir
        if DEFAULT_RUNTIME_DIR.exists():
            return DEFAULT_RUNTIME_DIR
        return LOCAL_RUNTIME_DIR

    @staticmethod
    def _resolve_output_dir(output_dir: Optional[Path]) -> Path:
        if output_dir is not None:
            return output_dir
        if DEFAULT_RUNTIME_DIR.exists():
            return DEFAULT_OUTPUT_DIR
        return LOCAL_OUTPUT_DIR

    def build_dashboard(self) -> Dict[str, Any]:
        ensure_dir(self.output_dir)

        raw_snapshots = safe_read_ndjson(self.snapshot_path)
        raw_journal = safe_read_ndjson(self.journal_path)

        snapshots = [EdgeDashboardNormalizer.normalize_snapshot(x) for x in raw_snapshots]
        journal = [EdgeDashboardNormalizer.normalize_journal(x) for x in raw_journal]

        summary = self._build_summary(snapshots=snapshots, journal=journal)
        scenario_edge_summary = self._build_scenario_edge_summary(snapshots=snapshots, journal=journal)
        time_edge_summary = self._build_time_edge_summary(snapshots=snapshots, journal=journal)
        csv_payloads = self._build_csv_payloads(snapshots=snapshots, journal=journal)

        self._write_outputs(
            summary=summary,
            scenario_edge_summary=scenario_edge_summary,
            time_edge_summary=time_edge_summary,
            csv_payloads=csv_payloads,
        )

        return {
            "runtime_dir": str(self.runtime_dir),
            "output_dir": str(self.output_dir),
            "snapshot_path": str(self.snapshot_path),
            "journal_path": str(self.journal_path),
            "snapshot_rows": len(snapshots),
            "journal_rows": len(journal),
            "summary_path": str(self.output_dir / SUMMARY_JSON_FILENAME),
            "scenario_edge_summary_path": str(self.output_dir / SCENARIO_EDGE_SUMMARY_JSON_FILENAME),
            "time_edge_summary_path": str(self.output_dir / TIME_EDGE_SUMMARY_JSON_FILENAME),
            "csv_files": [str(self.output_dir / name) for name in csv_payloads.keys()],
            "summary": summary,
            "scenario_edge_summary": scenario_edge_summary,
            "time_edge_summary": time_edge_summary,
        }

    # ----------------------------------------------------------------------------------
    # SUMMARY
    # ----------------------------------------------------------------------------------

    def _build_summary(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> Dict[str, Any]:
        total_snapshots = len(snapshots)
        total_journal = len(journal)

        symbols = sorted(set([s.symbol for s in snapshots] + [j.symbol for j in journal]))
        days = sorted(set([s.day for s in snapshots] + [j.day for j in journal]))

        market_regime_distribution = self._metric_market_regime_distribution(snapshots)
        scenario_frequency = self._metric_scenario_frequency(snapshots, journal)
        opportunity_rate = self._metric_opportunity_rate(snapshots)
        execution_completeness = self._metric_execution_completeness(snapshots, journal)
        consistency_score = self._metric_consistency_score(snapshots, journal)
        signal_lifecycle_stats = self._metric_signal_lifecycle_stats(journal)
        symbol_overview = self._metric_symbol_overview(
            snapshots=snapshots,
            journal=journal,
            opportunity_rate=opportunity_rate,
            execution_completeness=execution_completeness,
            consistency_score=consistency_score,
            signal_lifecycle=signal_lifecycle_stats,
        )
        daily_overview = self._metric_daily_overview(snapshots=snapshots, journal=journal)

        return {
            "generated_at_utc": utc_now_iso(),
            "source": {
                "runtime_dir": str(self.runtime_dir),
                "snapshot_path": str(self.snapshot_path),
                "journal_path": str(self.journal_path),
            },
            "coverage": {
                "snapshot_rows": total_snapshots,
                "journal_rows": total_journal,
                "symbols": symbols,
                "days": days,
                "symbols_count": len(symbols),
                "days_count": len(days),
            },
            "metrics": {
                "market_regime_distribution": market_regime_distribution,
                "scenario_frequency": scenario_frequency,
                "opportunity_rate": opportunity_rate,
                "execution_completeness": execution_completeness,
                "consistency_score": consistency_score,
                "signal_lifecycle_stats": signal_lifecycle_stats,
                "symbol_overview": symbol_overview,
                "daily_overview": daily_overview,
            },
        }

    # ----------------------------------------------------------------------------------
    # CORE METRICS
    # ----------------------------------------------------------------------------------

    def _metric_market_regime_distribution(
        self,
        snapshots: List[SnapshotRecord],
    ) -> Dict[str, Any]:
        total = len(snapshots)
        by_regime = Counter(s.market_state for s in snapshots)

        overall_rows = []
        for regime, count in sorted(by_regime.items(), key=lambda x: (-x[1], x[0])):
            overall_rows.append({
                "market_state": regime,
                "count": count,
                "pct": pct(count, total),
            })

        by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        grouped: Dict[str, List[SnapshotRecord]] = defaultdict(list)
        for s in snapshots:
            grouped[s.symbol].append(s)

        for symbol, rows in sorted(grouped.items()):
            symbol_total = len(rows)
            c = Counter(r.market_state for r in rows)
            by_symbol[symbol] = [
                {
                    "market_state": regime,
                    "count": count,
                    "pct": pct(count, symbol_total),
                }
                for regime, count in sorted(c.items(), key=lambda x: (-x[1], x[0]))
            ]

        return {
            "total_snapshots": total,
            "overall": overall_rows,
            "by_symbol": by_symbol,
        }

    def _metric_scenario_frequency(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> Dict[str, Any]:
        snapshot_counter = Counter(s.scenario for s in snapshots)
        journal_counter = Counter(j.scenario for j in journal if j.scenario != "UNKNOWN")

        combined = Counter()
        combined.update(snapshot_counter)
        combined.update(journal_counter)

        total_snapshot = sum(snapshot_counter.values())
        total_journal = sum(journal_counter.values())
        total_combined = sum(combined.values())

        return {
            "from_snapshots": [
                {"scenario": name, "count": count, "pct": pct(count, total_snapshot)}
                for name, count in sorted(snapshot_counter.items(), key=lambda x: (-x[1], x[0]))
            ],
            "from_journal": [
                {"scenario": name, "count": count, "pct": pct(count, total_journal)}
                for name, count in sorted(journal_counter.items(), key=lambda x: (-x[1], x[0]))
            ],
            "combined": [
                {"scenario": name, "count": count, "pct": pct(count, total_combined)}
                for name, count in sorted(combined.items(), key=lambda x: (-x[1], x[0]))
            ],
        }

    def _metric_opportunity_rate(
        self,
        snapshots: List[SnapshotRecord],
    ) -> Dict[str, Any]:
        total = len(snapshots)
        opportunities = [s for s in snapshots if s.opportunity_flag]

        by_symbol_rows = []
        grouped: Dict[str, List[SnapshotRecord]] = defaultdict(list)
        for s in snapshots:
            grouped[s.symbol].append(s)

        for symbol, rows in sorted(grouped.items()):
            total_symbol = len(rows)
            opp_symbol = sum(1 for r in rows if r.opportunity_flag)
            ready_symbol = sum(1 for r in rows if r.execution_ready_flag)
            by_symbol_rows.append({
                "symbol": symbol,
                "snapshot_count": total_symbol,
                "opportunity_count": opp_symbol,
                "opportunity_rate_pct": pct(opp_symbol, total_symbol),
                "execution_ready_count": ready_symbol,
                "execution_ready_rate_pct": pct(ready_symbol, total_symbol),
            })

        return {
            "overall": {
                "snapshot_count": total,
                "opportunity_count": len(opportunities),
                "opportunity_rate_pct": pct(len(opportunities), total),
                "execution_ready_count": sum(1 for s in snapshots if s.execution_ready_flag),
                "execution_ready_rate_pct": pct(
                    sum(1 for s in snapshots if s.execution_ready_flag),
                    total,
                ),
            },
            "by_symbol": by_symbol_rows,
        }

    def _metric_execution_completeness(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> Dict[str, Any]:
        readiness_by_signal: Dict[str, bool] = {}
        executed_by_signal: Dict[str, bool] = {}

        for s in snapshots:
            signal_id = normalize_str(get_first(s.raw, "signal_id", default=""))
            if signal_id and signal_id != "UNKNOWN":
                if s.execution_ready_flag or s.opportunity_flag:
                    readiness_by_signal[signal_id] = True

        for j in journal:
            signal_id = j.signal_id
            if signal_id == "UNKNOWN_SIGNAL":
                continue

            event_blob = " ".join([j.event_type.upper(), j.status.upper(), j.alert_type.upper()])

            if any(marker in event_blob for marker in ["OPEN", "TRIGGER", "EXECUT", "FILLED", "ACTIVE"]):
                executed_by_signal[signal_id] = True

            if any(marker in event_blob for marker in ["READY", "WATCH", "NEW", "ENTRY"]):
                readiness_by_signal[signal_id] = True

        ready_count = len(readiness_by_signal)
        executed_count = sum(1 for sid in readiness_by_signal if executed_by_signal.get(sid, False))

        ready_by_symbol: Dict[str, set] = defaultdict(set)
        executed_by_symbol: Dict[str, set] = defaultdict(set)

        for s in snapshots:
            signal_id = normalize_str(get_first(s.raw, "signal_id", default=""))
            if signal_id and signal_id != "UNKNOWN":
                if s.execution_ready_flag or s.opportunity_flag:
                    ready_by_symbol[s.symbol].add(signal_id)

        for j in journal:
            sid = j.signal_id
            if sid == "UNKNOWN_SIGNAL":
                continue

            event_blob = " ".join([j.event_type.upper(), j.status.upper(), j.alert_type.upper()])

            if any(marker in event_blob for marker in ["READY", "WATCH", "NEW", "ENTRY"]):
                ready_by_symbol[j.symbol].add(sid)

            if any(marker in event_blob for marker in ["OPEN", "TRIGGER", "EXECUT", "FILLED", "ACTIVE"]):
                executed_by_symbol[j.symbol].add(sid)

        by_symbol_rows = []
        all_symbols = sorted(set(list(ready_by_symbol.keys()) + list(executed_by_symbol.keys())))
        for symbol in all_symbols:
            ready = len(ready_by_symbol.get(symbol, set()))
            executed = len(ready_by_symbol.get(symbol, set()) & executed_by_symbol.get(symbol, set()))
            by_symbol_rows.append({
                "symbol": symbol,
                "ready_signal_count": ready,
                "executed_signal_count": executed,
                "execution_completeness_pct": pct(executed, ready),
            })

        return {
            "overall": {
                "ready_signal_count": ready_count,
                "executed_signal_count": executed_count,
                "execution_completeness_pct": pct(executed_count, ready_count),
            },
            "by_symbol": by_symbol_rows,
        }

    def _metric_consistency_score(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> Dict[str, Any]:
        snapshot_scores = [s.consistency_score for s in snapshots if s.consistency_score is not None]
        journal_scores = [j.consistency_score for j in journal if j.consistency_score is not None]

        by_symbol_values: Dict[str, List[float]] = defaultdict(list)
        for s in snapshots:
            if s.consistency_score is not None:
                by_symbol_values[s.symbol].append(s.consistency_score)
        for j in journal:
            if j.consistency_score is not None:
                by_symbol_values[j.symbol].append(j.consistency_score)

        by_symbol_rows = []
        for symbol, values in sorted(by_symbol_values.items()):
            by_symbol_rows.append({
                "symbol": symbol,
                "avg_consistency_score": avg(values),
                "min_consistency_score": round(min(values), 4) if values else 0.0,
                "max_consistency_score": round(max(values), 4) if values else 0.0,
                "samples": len(values),
            })

        all_scores = snapshot_scores + journal_scores

        if not all_scores:
            return self._infer_structural_consistency(snapshots, journal)

        return {
            "overall": {
                "avg_consistency_score": avg(all_scores),
                "min_consistency_score": round(min(all_scores), 4) if all_scores else 0.0,
                "max_consistency_score": round(max(all_scores), 4) if all_scores else 0.0,
                "samples": len(all_scores),
                "method": "explicit_consistency_fields",
            },
            "by_symbol": by_symbol_rows,
        }

    def _infer_structural_consistency(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> Dict[str, Any]:
        def row_score(values: List[str]) -> float:
            known = sum(1 for x in values if x not in {"UNKNOWN", "UNKNOWN_SIGNAL", "UNKNOWN_DATE", ""})
            return round(known / 5.0, 4)

        scores: List[Tuple[str, float]] = []

        for s in snapshots:
            scores.append((s.symbol, row_score([s.scenario, s.market_state, s.direction, s.htf_bias, s.signal_class])))

        for j in journal:
            scores.append((j.symbol, row_score([j.scenario, j.market_state, j.direction, j.htf_bias, j.signal_class])))

        if not scores:
            return {
                "overall": {
                    "avg_consistency_score": 0.0,
                    "min_consistency_score": 0.0,
                    "max_consistency_score": 0.0,
                    "samples": 0,
                    "method": "structural_fallback",
                },
                "by_symbol": [],
            }

        by_symbol_map: Dict[str, List[float]] = defaultdict(list)
        for symbol, score in scores:
            by_symbol_map[symbol].append(score)

        by_symbol_rows = []
        for symbol, vals in sorted(by_symbol_map.items()):
            by_symbol_rows.append({
                "symbol": symbol,
                "avg_consistency_score": avg(vals),
                "min_consistency_score": round(min(vals), 4),
                "max_consistency_score": round(max(vals), 4),
                "samples": len(vals),
            })

        all_vals = [score for _, score in scores]
        return {
            "overall": {
                "avg_consistency_score": avg(all_vals),
                "min_consistency_score": round(min(all_vals), 4),
                "max_consistency_score": round(max(all_vals), 4),
                "samples": len(all_vals),
                "method": "structural_fallback",
            },
            "by_symbol": by_symbol_rows,
        }

    def _metric_signal_lifecycle_stats(
        self,
        journal: List[JournalRecord],
    ) -> Dict[str, Any]:
        by_signal: Dict[str, List[JournalRecord]] = defaultdict(list)
        for j in journal:
            if j.signal_id != "UNKNOWN_SIGNAL":
                by_signal[j.signal_id].append(j)

        total_signals = len(by_signal)

        stage_counts = {
            "created_or_detected": 0,
            "watch": 0,
            "ready": 0,
            "triggered_or_opened": 0,
            "closed_or_resolved": 0,
        }

        per_symbol_stage_counts: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {
                "created_or_detected": 0,
                "watch": 0,
                "ready": 0,
                "triggered_or_opened": 0,
                "closed_or_resolved": 0,
            }
        )

        for _, events in by_signal.items():
            blob = " | ".join([f"{e.event_type.upper()} {e.status.upper()} {e.alert_type.upper()}" for e in events])
            symbol = events[0].symbol

            stage_counts["created_or_detected"] += 1
            per_symbol_stage_counts[symbol]["created_or_detected"] += 1

            if any(m in blob for m in ["WATCH", "WATCH_NEW"]):
                stage_counts["watch"] += 1
                per_symbol_stage_counts[symbol]["watch"] += 1

            if any(m in blob for m in ["READY", "ENTRY_READY", "TRADE_READY"]):
                stage_counts["ready"] += 1
                per_symbol_stage_counts[symbol]["ready"] += 1

            if any(m in blob for m in ["TRIGGER", "OPEN", "ACTIVE", "FILLED", "EXECUT"]):
                stage_counts["triggered_or_opened"] += 1
                per_symbol_stage_counts[symbol]["triggered_or_opened"] += 1

            if any(m in blob for m in ["CLOSE", "CLOSED", "EXIT", "RESOLVED", "INVALIDATED", "CANCELLED"]):
                stage_counts["closed_or_resolved"] += 1
                per_symbol_stage_counts[symbol]["closed_or_resolved"] += 1

        by_symbol_rows = []
        for symbol, counts in sorted(per_symbol_stage_counts.items()):
            created = counts["created_or_detected"]
            by_symbol_rows.append({
                "symbol": symbol,
                **counts,
                "watch_pct": pct(counts["watch"], created),
                "ready_pct": pct(counts["ready"], created),
                "triggered_or_opened_pct": pct(counts["triggered_or_opened"], created),
                "closed_or_resolved_pct": pct(counts["closed_or_resolved"], created),
            })

        return {
            "overall": {
                "total_unique_signals": total_signals,
                **stage_counts,
                "watch_pct": pct(stage_counts["watch"], total_signals),
                "ready_pct": pct(stage_counts["ready"], total_signals),
                "triggered_or_opened_pct": pct(stage_counts["triggered_or_opened"], total_signals),
                "closed_or_resolved_pct": pct(stage_counts["closed_or_resolved"], total_signals),
            },
            "by_symbol": by_symbol_rows,
        }

    def _metric_symbol_overview(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
        opportunity_rate: Dict[str, Any],
        execution_completeness: Dict[str, Any],
        consistency_score: Dict[str, Any],
        signal_lifecycle: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        snapshot_group: Dict[str, List[SnapshotRecord]] = defaultdict(list)
        journal_group: Dict[str, List[JournalRecord]] = defaultdict(list)

        for s in snapshots:
            snapshot_group[s.symbol].append(s)
        for j in journal:
            journal_group[j.symbol].append(j)

        opp_map = {row["symbol"]: row for row in opportunity_rate.get("by_symbol", [])}
        exec_map = {row["symbol"]: row for row in execution_completeness.get("by_symbol", [])}
        cons_map = {row["symbol"]: row for row in consistency_score.get("by_symbol", [])}
        life_map = {row["symbol"]: row for row in signal_lifecycle.get("by_symbol", [])}

        rows = []
        all_symbols = sorted(set(snapshot_group.keys()) | set(journal_group.keys()))

        for symbol in all_symbols:
            srows = snapshot_group.get(symbol, [])
            jrows = journal_group.get(symbol, [])

            dominant_regime = "UNKNOWN"
            dominant_scenario = "UNKNOWN"

            if srows:
                dominant_regime = Counter(x.market_state for x in srows).most_common(1)[0][0]
                dominant_scenario = Counter(x.scenario for x in srows).most_common(1)[0][0]

            rows.append({
                "symbol": symbol,
                "snapshot_count": len(srows),
                "journal_count": len(jrows),
                "dominant_market_state": dominant_regime,
                "dominant_scenario": dominant_scenario,
                "opportunity_rate_pct": opp_map.get(symbol, {}).get("opportunity_rate_pct", 0.0),
                "execution_ready_rate_pct": opp_map.get(symbol, {}).get("execution_ready_rate_pct", 0.0),
                "execution_completeness_pct": exec_map.get(symbol, {}).get("execution_completeness_pct", 0.0),
                "avg_consistency_score": cons_map.get(symbol, {}).get("avg_consistency_score", 0.0),
                "unique_signals_created": life_map.get(symbol, {}).get("created_or_detected", 0),
                "watch_pct": life_map.get(symbol, {}).get("watch_pct", 0.0),
                "ready_pct": life_map.get(symbol, {}).get("ready_pct", 0.0),
                "triggered_or_opened_pct": life_map.get(symbol, {}).get("triggered_or_opened_pct", 0.0),
                "closed_or_resolved_pct": life_map.get(symbol, {}).get("closed_or_resolved_pct", 0.0),
            })

        return rows

    def _metric_daily_overview(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> List[Dict[str, Any]]:
        snap_by_day: Dict[str, List[SnapshotRecord]] = defaultdict(list)
        jour_by_day: Dict[str, List[JournalRecord]] = defaultdict(list)

        for s in snapshots:
            snap_by_day[s.day].append(s)
        for j in journal:
            jour_by_day[j.day].append(j)

        all_days = sorted(set(snap_by_day.keys()) | set(jour_by_day.keys()))
        rows = []

        for day in all_days:
            srows = snap_by_day.get(day, [])
            jrows = jour_by_day.get(day, [])

            dominant_regime = "UNKNOWN"
            top_scenario = "UNKNOWN"

            if srows:
                dominant_regime = Counter(x.market_state for x in srows).most_common(1)[0][0]
                top_scenario = Counter(x.scenario for x in srows).most_common(1)[0][0]

            unique_signals = len({x.signal_id for x in jrows if x.signal_id != "UNKNOWN_SIGNAL"})
            opportunities = sum(1 for x in srows if x.opportunity_flag)
            ready = sum(1 for x in srows if x.execution_ready_flag)

            rows.append({
                "day": day,
                "snapshot_count": len(srows),
                "journal_count": len(jrows),
                "unique_signal_count": unique_signals,
                "opportunity_count": opportunities,
                "opportunity_rate_pct": pct(opportunities, len(srows)),
                "execution_ready_count": ready,
                "execution_ready_rate_pct": pct(ready, len(srows)),
                "dominant_market_state": dominant_regime,
                "top_scenario": top_scenario,
                "avg_snapshot_probability": avg([x.probability for x in srows if x.probability is not None]),
                "avg_snapshot_consistency": avg([x.consistency_score for x in srows if x.consistency_score is not None]),
            })

        return rows

    # ----------------------------------------------------------------------------------
    # SCENARIO EDGE V2
    # ----------------------------------------------------------------------------------

    def _build_scenario_edge_summary(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> Dict[str, Any]:
        matrix_rows = self._metric_scenario_edge_matrix(snapshots, journal)

        ranked = sorted(
            matrix_rows,
            key=lambda x: (
                -x["edge_score"],
                -x["execution_completeness_pct"],
                -x["opportunity_rate_pct"],
                -x["snapshot_count"],
                x["symbol"],
                x["scenario"],
            ),
        )

        top_edges = ranked[:15]
        tradeable_edges = [
            row for row in ranked
            if row["snapshot_count"] >= 3
            and row["unique_signal_count"] >= 1
            and row["opportunity_rate_pct"] >= 20
        ][:20]

        by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in ranked:
            by_symbol[row["symbol"]].append({
                "scenario": row["scenario"],
                "edge_score": row["edge_score"],
                "snapshot_count": row["snapshot_count"],
                "journal_count": row["journal_count"],
                "unique_signal_count": row["unique_signal_count"],
                "opportunity_rate_pct": row["opportunity_rate_pct"],
                "execution_completeness_pct": row["execution_completeness_pct"],
                "avg_probability": row["avg_probability"],
                "avg_consistency_score": row["avg_consistency_score"],
                "dominant_market_state": row["dominant_market_state"],
                "dominant_direction": row["dominant_direction"],
            })

        return {
            "generated_at_utc": utc_now_iso(),
            "rows": len(matrix_rows),
            "top_edges": top_edges,
            "tradeable_edges": tradeable_edges,
            "by_symbol": dict(by_symbol),
        }

    def _metric_scenario_edge_matrix(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> List[Dict[str, Any]]:
        snap_groups: Dict[Tuple[str, str], List[SnapshotRecord]] = defaultdict(list)
        jour_groups: Dict[Tuple[str, str], List[JournalRecord]] = defaultdict(list)

        for s in snapshots:
            snap_groups[(s.symbol, s.scenario)].append(s)

        for j in journal:
            jour_groups[(j.symbol, j.scenario)].append(j)

        all_keys = sorted(set(snap_groups.keys()) | set(jour_groups.keys()))
        rows: List[Dict[str, Any]] = []

        for symbol, scenario in all_keys:
            srows = snap_groups.get((symbol, scenario), [])
            jrows = jour_groups.get((symbol, scenario), [])

            snapshot_count = len(srows)
            journal_count = len(jrows)

            opportunity_count = sum(1 for x in srows if x.opportunity_flag)
            execution_ready_count = sum(1 for x in srows if x.execution_ready_flag)

            avg_probability = avg([x.probability for x in srows if x.probability is not None])

            explicit_cons_scores = [x.consistency_score for x in srows if x.consistency_score is not None]
            explicit_cons_scores += [x.consistency_score for x in jrows if x.consistency_score is not None]
            avg_consistency_score = avg(explicit_cons_scores)

            dominant_market_state = "UNKNOWN"
            if srows:
                dominant_market_state = Counter(x.market_state for x in srows).most_common(1)[0][0]
            elif jrows:
                dominant_market_state = Counter(x.market_state for x in jrows).most_common(1)[0][0]

            dominant_direction = "UNKNOWN"
            dir_values = [x.direction for x in srows] + [x.direction for x in jrows]
            if dir_values:
                dominant_direction = Counter(dir_values).most_common(1)[0][0]

            unique_signal_ids = {j.signal_id for j in jrows if j.signal_id != "UNKNOWN_SIGNAL"}
            unique_signal_count = len(unique_signal_ids)

            ready_signal_ids = set()
            executed_signal_ids = set()
            watch_signal_ids = set()
            closed_signal_ids = set()

            for j in jrows:
                sid = j.signal_id
                if sid == "UNKNOWN_SIGNAL":
                    continue

                blob = " ".join([j.event_type.upper(), j.status.upper(), j.alert_type.upper()])

                if any(m in blob for m in ["WATCH", "WATCH_NEW"]):
                    watch_signal_ids.add(sid)

                if any(m in blob for m in ["READY", "ENTRY_READY", "TRADE_READY", "ENTRY"]):
                    ready_signal_ids.add(sid)

                if any(m in blob for m in ["OPEN", "TRIGGER", "ACTIVE", "FILLED", "EXECUT"]):
                    executed_signal_ids.add(sid)

                if any(m in blob for m in ["CLOSE", "CLOSED", "EXIT", "RESOLVED", "INVALIDATED", "CANCELLED"]):
                    closed_signal_ids.add(sid)

            for s in srows:
                sid = normalize_str(get_first(s.raw, "signal_id", default=""))
                if sid and sid != "UNKNOWN":
                    if s.opportunity_flag or s.execution_ready_flag:
                        ready_signal_ids.add(sid)

            opportunity_rate_pct = pct(opportunity_count, snapshot_count)
            execution_ready_rate_pct = pct(execution_ready_count, snapshot_count)
            execution_completeness_pct = pct(len(executed_signal_ids), len(ready_signal_ids))

            watch_pct = pct(len(watch_signal_ids), unique_signal_count)
            ready_pct = pct(len(ready_signal_ids), unique_signal_count)
            triggered_or_opened_pct = pct(len(executed_signal_ids), unique_signal_count)
            closed_or_resolved_pct = pct(len(closed_signal_ids), unique_signal_count)

            edge_score = self._calculate_edge_score(
                snapshot_count=snapshot_count,
                opportunity_rate_pct=opportunity_rate_pct,
                execution_completeness_pct=execution_completeness_pct,
                avg_probability=avg_probability,
                avg_consistency_score=avg_consistency_score,
                triggered_or_opened_pct=triggered_or_opened_pct,
            )

            rows.append({
                "symbol": symbol,
                "scenario": scenario,
                "snapshot_count": snapshot_count,
                "journal_count": journal_count,
                "unique_signal_count": unique_signal_count,
                "opportunity_count": opportunity_count,
                "opportunity_rate_pct": opportunity_rate_pct,
                "execution_ready_count": execution_ready_count,
                "execution_ready_rate_pct": execution_ready_rate_pct,
                "ready_signal_count": len(ready_signal_ids),
                "executed_signal_count": len(executed_signal_ids),
                "execution_completeness_pct": execution_completeness_pct,
                "avg_probability": avg_probability,
                "avg_consistency_score": avg_consistency_score,
                "dominant_market_state": dominant_market_state,
                "dominant_direction": dominant_direction,
                "watch_pct": watch_pct,
                "ready_pct": ready_pct,
                "triggered_or_opened_pct": triggered_or_opened_pct,
                "closed_or_resolved_pct": closed_or_resolved_pct,
                "edge_score": edge_score,
            })

        return sorted(
            rows,
            key=lambda x: (
                -x["edge_score"],
                -x["execution_completeness_pct"],
                -x["opportunity_rate_pct"],
                -x["snapshot_count"],
                x["symbol"],
                x["scenario"],
            ),
        )

    # ----------------------------------------------------------------------------------
    # TIME EDGE V3
    # ----------------------------------------------------------------------------------

    def _build_time_edge_summary(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> Dict[str, Any]:
        by_hour = self._metric_time_edge_by_hour(snapshots, journal)
        by_weekday = self._metric_time_edge_by_weekday(snapshots, journal)
        by_session = self._metric_time_edge_by_session(snapshots, journal)

        best_hours = sorted(
            [row for row in by_hour if row["snapshot_count"] > 0],
            key=lambda x: (-x["time_edge_score"], -x["snapshot_count"], x["hour_utc"]),
        )[:10]

        best_weekdays = sorted(
            [row for row in by_weekday if row["snapshot_count"] > 0],
            key=lambda x: (-x["time_edge_score"], -x["snapshot_count"], x["weekday_index"]),
        )

        best_sessions = sorted(
            [row for row in by_session if row["snapshot_count"] > 0],
            key=lambda x: (-x["time_edge_score"], -x["snapshot_count"], x["session"]),
        )

        return {
            "generated_at_utc": utc_now_iso(),
            "timezone_basis": "UTC",
            "session_definition": {
                "ASIA": "00:00-07:59 UTC",
                "LONDON": "08:00-12:59 UTC",
                "NEW_YORK": "13:00-21:59 UTC",
                "LATE_US": "22:00-23:59 UTC",
            },
            "best_hours": best_hours,
            "best_weekdays": best_weekdays,
            "best_sessions": best_sessions,
        }

    def _metric_time_edge_by_hour(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> List[Dict[str, Any]]:
        rows = []
        for hour in range(24):
            srows = [s for s in snapshots if dt_hour_utc(s.timestamp) == hour]
            jrows = [j for j in journal if dt_hour_utc(j.timestamp) == hour]
            rows.append(self._build_time_bucket_row(
                bucket_type="hour",
                bucket_value=hour,
                bucket_label=f"{hour:02d}:00 UTC",
                srows=srows,
                jrows=jrows,
            ))
        return rows

    def _metric_time_edge_by_weekday(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> List[Dict[str, Any]]:
        rows = []
        for wd in range(7):
            srows = [s for s in snapshots if dt_weekday_utc(s.timestamp) == wd]
            jrows = [j for j in journal if dt_weekday_utc(j.timestamp) == wd]
            row = self._build_time_bucket_row(
                bucket_type="weekday",
                bucket_value=wd,
                bucket_label=weekday_name(wd),
                srows=srows,
                jrows=jrows,
            )
            row["weekday_index"] = wd
            row["weekday_name"] = weekday_name(wd)
            rows.append(row)
        return rows

    def _metric_time_edge_by_session(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> List[Dict[str, Any]]:
        session_order = ["ASIA", "LONDON", "NEW_YORK", "LATE_US", "UNKNOWN"]
        rows = []
        for session in session_order:
            srows = [s for s in snapshots if detect_session_utc(s.timestamp) == session]
            jrows = [j for j in journal if detect_session_utc(j.timestamp) == session]
            rows.append(self._build_time_bucket_row(
                bucket_type="session",
                bucket_value=session,
                bucket_label=session,
                srows=srows,
                jrows=jrows,
            ))
        return rows

    def _build_time_bucket_row(
        self,
        bucket_type: str,
        bucket_value: Any,
        bucket_label: str,
        srows: List[SnapshotRecord],
        jrows: List[JournalRecord],
    ) -> Dict[str, Any]:
        snapshot_count = len(srows)
        journal_count = len(jrows)

        opportunity_count = sum(1 for s in srows if s.opportunity_flag)
        execution_ready_count = sum(1 for s in srows if s.execution_ready_flag)

        avg_probability = avg([s.probability for s in srows if s.probability is not None])

        cons_scores = [s.consistency_score for s in srows if s.consistency_score is not None]
        cons_scores += [j.consistency_score for j in jrows if j.consistency_score is not None]
        avg_consistency_score = avg(cons_scores)

        unique_signal_ids = {j.signal_id for j in jrows if j.signal_id != "UNKNOWN_SIGNAL"}
        unique_signal_count = len(unique_signal_ids)

        ready_signal_ids = set()
        executed_signal_ids = set()
        watch_signal_ids = set()
        closed_signal_ids = set()

        for j in jrows:
            sid = j.signal_id
            if sid == "UNKNOWN_SIGNAL":
                continue

            blob = " ".join([j.event_type.upper(), j.status.upper(), j.alert_type.upper()])

            if any(m in blob for m in ["WATCH", "WATCH_NEW"]):
                watch_signal_ids.add(sid)

            if any(m in blob for m in ["READY", "ENTRY_READY", "TRADE_READY", "ENTRY"]):
                ready_signal_ids.add(sid)

            if any(m in blob for m in ["OPEN", "TRIGGER", "ACTIVE", "FILLED", "EXECUT"]):
                executed_signal_ids.add(sid)

            if any(m in blob for m in ["CLOSE", "CLOSED", "EXIT", "RESOLVED", "INVALIDATED", "CANCELLED"]):
                closed_signal_ids.add(sid)

        for s in srows:
            sid = normalize_str(get_first(s.raw, "signal_id", default=""))
            if sid and sid != "UNKNOWN":
                if s.opportunity_flag or s.execution_ready_flag:
                    ready_signal_ids.add(sid)

        dominant_market_state = "UNKNOWN"
        if srows:
            dominant_market_state = Counter(x.market_state for x in srows).most_common(1)[0][0]

        dominant_scenario = "UNKNOWN"
        scen_values = [x.scenario for x in srows] + [x.scenario for x in jrows if x.scenario != "UNKNOWN"]
        if scen_values:
            dominant_scenario = Counter(scen_values).most_common(1)[0][0]

        opportunity_rate_pct = pct(opportunity_count, snapshot_count)
        execution_ready_rate_pct = pct(execution_ready_count, snapshot_count)
        execution_completeness_pct = pct(len(executed_signal_ids), len(ready_signal_ids))
        watch_pct = pct(len(watch_signal_ids), unique_signal_count)
        ready_pct = pct(len(ready_signal_ids), unique_signal_count)
        triggered_or_opened_pct = pct(len(executed_signal_ids), unique_signal_count)
        closed_or_resolved_pct = pct(len(closed_signal_ids), unique_signal_count)

        time_edge_score = self._calculate_edge_score(
            snapshot_count=snapshot_count,
            opportunity_rate_pct=opportunity_rate_pct,
            execution_completeness_pct=execution_completeness_pct,
            avg_probability=avg_probability,
            avg_consistency_score=avg_consistency_score,
            triggered_or_opened_pct=triggered_or_opened_pct,
        )

        row = {
            "bucket_type": bucket_type,
            "bucket_value": bucket_value,
            "bucket_label": bucket_label,
            "snapshot_count": snapshot_count,
            "journal_count": journal_count,
            "unique_signal_count": unique_signal_count,
            "opportunity_count": opportunity_count,
            "opportunity_rate_pct": opportunity_rate_pct,
            "execution_ready_count": execution_ready_count,
            "execution_ready_rate_pct": execution_ready_rate_pct,
            "ready_signal_count": len(ready_signal_ids),
            "executed_signal_count": len(executed_signal_ids),
            "execution_completeness_pct": execution_completeness_pct,
            "avg_probability": avg_probability,
            "avg_consistency_score": avg_consistency_score,
            "dominant_market_state": dominant_market_state,
            "dominant_scenario": dominant_scenario,
            "watch_pct": watch_pct,
            "ready_pct": ready_pct,
            "triggered_or_opened_pct": triggered_or_opened_pct,
            "closed_or_resolved_pct": closed_or_resolved_pct,
            "time_edge_score": time_edge_score,
        }

        if bucket_type == "hour":
            row["hour_utc"] = bucket_value
        elif bucket_type == "session":
            row["session"] = bucket_value

        return row

    # ----------------------------------------------------------------------------------
    # EDGE SCORE
    # ----------------------------------------------------------------------------------

    def _calculate_edge_score(
        self,
        snapshot_count: int,
        opportunity_rate_pct: float,
        execution_completeness_pct: float,
        avg_probability: float,
        avg_consistency_score: float,
        triggered_or_opened_pct: float,
    ) -> float:
        sample_factor = min(snapshot_count / 10.0, 1.0)

        opp = opportunity_rate_pct / 100.0
        exec_comp = execution_completeness_pct / 100.0
        trig = triggered_or_opened_pct / 100.0

        prob = avg_probability / 100.0 if avg_probability > 1.0 else avg_probability
        prob = max(0.0, min(prob, 1.0))

        cons = avg_consistency_score / 100.0 if avg_consistency_score > 1.0 else avg_consistency_score
        cons = max(0.0, min(cons, 1.0))

        raw_score = (
            opp * 0.30 +
            exec_comp * 0.25 +
            trig * 0.15 +
            prob * 0.15 +
            cons * 0.15
        )

        final_score = raw_score * (0.45 + 0.55 * sample_factor)
        return round(final_score * 100.0, 2)

    # ----------------------------------------------------------------------------------
    # CSV PAYLOADS
    # ----------------------------------------------------------------------------------

    def _build_csv_payloads(
        self,
        snapshots: List[SnapshotRecord],
        journal: List[JournalRecord],
    ) -> Dict[str, List[Dict[str, Any]]]:
        market_regime = self._metric_market_regime_distribution(snapshots)
        scenario_frequency = self._metric_scenario_frequency(snapshots, journal)
        opportunity_rate = self._metric_opportunity_rate(snapshots)
        execution_completeness = self._metric_execution_completeness(snapshots, journal)
        consistency_score = self._metric_consistency_score(snapshots, journal)
        signal_lifecycle = self._metric_signal_lifecycle_stats(journal)
        symbol_overview = self._metric_symbol_overview(
            snapshots=snapshots,
            journal=journal,
            opportunity_rate=opportunity_rate,
            execution_completeness=execution_completeness,
            consistency_score=consistency_score,
            signal_lifecycle=signal_lifecycle,
        )
        daily_overview = self._metric_daily_overview(snapshots=snapshots, journal=journal)
        scenario_edge_matrix = self._metric_scenario_edge_matrix(snapshots=snapshots, journal=journal)
        time_edge_by_hour = self._metric_time_edge_by_hour(snapshots=snapshots, journal=journal)
        time_edge_by_weekday = self._metric_time_edge_by_weekday(snapshots=snapshots, journal=journal)
        time_edge_by_session = self._metric_time_edge_by_session(snapshots=snapshots, journal=journal)

        market_rows: List[Dict[str, Any]] = []
        for row in market_regime["overall"]:
            market_rows.append({"scope": "overall", "symbol": "ALL", **row})
        for symbol, items in market_regime["by_symbol"].items():
            for row in items:
                market_rows.append({"scope": "symbol", "symbol": symbol, **row})

        scenario_rows: List[Dict[str, Any]] = []
        for source_key in ["from_snapshots", "from_journal", "combined"]:
            for row in scenario_frequency[source_key]:
                scenario_rows.append({"source": source_key, **row})

        execution_rows: List[Dict[str, Any]] = [{"scope": "overall", "symbol": "ALL", **execution_completeness["overall"]}]
        execution_rows.extend({"scope": "symbol", **row} for row in execution_completeness["by_symbol"])

        consistency_rows: List[Dict[str, Any]] = [{"scope": "overall", "symbol": "ALL", **consistency_score["overall"]}]
        consistency_rows.extend({"scope": "symbol", **row} for row in consistency_score["by_symbol"])

        lifecycle_rows: List[Dict[str, Any]] = [{"scope": "overall", "symbol": "ALL", **signal_lifecycle["overall"]}]
        lifecycle_rows.extend({"scope": "symbol", **row} for row in signal_lifecycle["by_symbol"])

        opportunity_rows: List[Dict[str, Any]] = [{"scope": "overall", "symbol": "ALL", **opportunity_rate["overall"]}]
        opportunity_rows.extend({"scope": "symbol", **row} for row in opportunity_rate["by_symbol"])

        return {
            CSV_MARKET_REGIME: market_rows,
            CSV_SCENARIO_FREQUENCY: scenario_rows,
            CSV_OPPORTUNITY_RATE: opportunity_rows,
            CSV_EXECUTION_COMPLETENESS: execution_rows,
            CSV_CONSISTENCY: consistency_rows,
            CSV_SIGNAL_LIFECYCLE: lifecycle_rows,
            CSV_SYMBOL_OVERVIEW: symbol_overview,
            CSV_DAILY_OVERVIEW: daily_overview,
            CSV_SCENARIO_EDGE_MATRIX: scenario_edge_matrix,
            CSV_TIME_EDGE_BY_HOUR: time_edge_by_hour,
            CSV_TIME_EDGE_BY_WEEKDAY: time_edge_by_weekday,
            CSV_TIME_EDGE_BY_SESSION: time_edge_by_session,
        }

    # ----------------------------------------------------------------------------------
    # WRITE OUTPUTS
    # ----------------------------------------------------------------------------------

    def _write_outputs(
        self,
        summary: Dict[str, Any],
        scenario_edge_summary: Dict[str, Any],
        time_edge_summary: Dict[str, Any],
        csv_payloads: Dict[str, List[Dict[str, Any]]],
    ) -> None:
        write_json(self.output_dir / SUMMARY_JSON_FILENAME, summary)
        write_json(self.output_dir / SCENARIO_EDGE_SUMMARY_JSON_FILENAME, scenario_edge_summary)
        write_json(self.output_dir / TIME_EDGE_SUMMARY_JSON_FILENAME, time_edge_summary)

        csv_fields = {
            CSV_MARKET_REGIME: [
                "scope", "symbol", "market_state", "count", "pct",
            ],
            CSV_SCENARIO_FREQUENCY: [
                "source", "scenario", "count", "pct",
            ],
            CSV_OPPORTUNITY_RATE: [
                "scope", "symbol", "snapshot_count", "opportunity_count",
                "opportunity_rate_pct", "execution_ready_count", "execution_ready_rate_pct",
            ],
            CSV_EXECUTION_COMPLETENESS: [
                "scope", "symbol", "ready_signal_count", "executed_signal_count",
                "execution_completeness_pct",
            ],
            CSV_CONSISTENCY: [
                "scope", "symbol", "avg_consistency_score", "min_consistency_score",
                "max_consistency_score", "samples", "method",
            ],
            CSV_SIGNAL_LIFECYCLE: [
                "scope", "symbol", "total_unique_signals", "created_or_detected",
                "watch", "ready", "triggered_or_opened", "closed_or_resolved",
                "watch_pct", "ready_pct", "triggered_or_opened_pct", "closed_or_resolved_pct",
            ],
            CSV_SYMBOL_OVERVIEW: [
                "symbol", "snapshot_count", "journal_count", "dominant_market_state",
                "dominant_scenario", "opportunity_rate_pct", "execution_ready_rate_pct",
                "execution_completeness_pct", "avg_consistency_score",
                "unique_signals_created", "watch_pct", "ready_pct",
                "triggered_or_opened_pct", "closed_or_resolved_pct",
            ],
            CSV_DAILY_OVERVIEW: [
                "day", "snapshot_count", "journal_count", "unique_signal_count",
                "opportunity_count", "opportunity_rate_pct", "execution_ready_count",
                "execution_ready_rate_pct", "dominant_market_state", "top_scenario",
                "avg_snapshot_probability", "avg_snapshot_consistency",
            ],
            CSV_SCENARIO_EDGE_MATRIX: [
                "symbol", "scenario", "snapshot_count", "journal_count", "unique_signal_count",
                "opportunity_count", "opportunity_rate_pct", "execution_ready_count",
                "execution_ready_rate_pct", "ready_signal_count", "executed_signal_count",
                "execution_completeness_pct", "avg_probability", "avg_consistency_score",
                "dominant_market_state", "dominant_direction", "watch_pct", "ready_pct",
                "triggered_or_opened_pct", "closed_or_resolved_pct", "edge_score",
            ],
            CSV_TIME_EDGE_BY_HOUR: [
                "bucket_type", "bucket_value", "bucket_label", "hour_utc",
                "snapshot_count", "journal_count", "unique_signal_count",
                "opportunity_count", "opportunity_rate_pct", "execution_ready_count",
                "execution_ready_rate_pct", "ready_signal_count", "executed_signal_count",
                "execution_completeness_pct", "avg_probability", "avg_consistency_score",
                "dominant_market_state", "dominant_scenario", "watch_pct", "ready_pct",
                "triggered_or_opened_pct", "closed_or_resolved_pct", "time_edge_score",
            ],
            CSV_TIME_EDGE_BY_WEEKDAY: [
                "bucket_type", "bucket_value", "bucket_label", "weekday_index", "weekday_name",
                "snapshot_count", "journal_count", "unique_signal_count",
                "opportunity_count", "opportunity_rate_pct", "execution_ready_count",
                "execution_ready_rate_pct", "ready_signal_count", "executed_signal_count",
                "execution_completeness_pct", "avg_probability", "avg_consistency_score",
                "dominant_market_state", "dominant_scenario", "watch_pct", "ready_pct",
                "triggered_or_opened_pct", "closed_or_resolved_pct", "time_edge_score",
            ],
            CSV_TIME_EDGE_BY_SESSION: [
                "bucket_type", "bucket_value", "bucket_label", "session",
                "snapshot_count", "journal_count", "unique_signal_count",
                "opportunity_count", "opportunity_rate_pct", "execution_ready_count",
                "execution_ready_rate_pct", "ready_signal_count", "executed_signal_count",
                "execution_completeness_pct", "avg_probability", "avg_consistency_score",
                "dominant_market_state", "dominant_scenario", "watch_pct", "ready_pct",
                "triggered_or_opened_pct", "closed_or_resolved_pct", "time_edge_score",
            ],
        }

        for filename, rows in csv_payloads.items():
            fields = csv_fields[filename]
            normalized_rows = []
            for row in rows:
                normalized = dict(row)
                for field in fields:
                    normalized.setdefault(field, "")
                normalized_rows.append(normalized)
            write_csv(self.output_dir / filename, normalized_rows, fields)