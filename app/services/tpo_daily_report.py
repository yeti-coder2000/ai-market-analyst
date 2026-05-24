from __future__ import annotations

import argparse
import json
import os
from collections import Counter, deque
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


DEFAULT_TIMEZONE = "Europe/Kyiv"


def _runtime_dir() -> Path:
    raw = os.getenv("RUNTIME_DIR")
    if raw:
        return Path(raw)

    try:
        from app.core.settings import settings

        value = getattr(settings, "runtime_dir", None)
        if value:
            return Path(value)
    except Exception:
        pass

    render_runtime = Path("/var/data/runtime")
    if render_runtime.exists():
        return render_runtime

    return Path("runtime")


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Counter):
        return dict(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _iter_ndjson(path: Path):
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                yield {
                    "event_type": "bad_json_line",
                    "line_no": line_no,
                    "raw": line[:500],
                }


def _target_window(report_date: date, tz_name: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    start_local = datetime.combine(report_date, time(0, 0), tzinfo=tz)
    end_local = datetime.combine(report_date, time(23, 59, 59, 999999), tzinfo=tz)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _event_in_window(event: dict[str, Any], start_utc: datetime, end_utc: datetime) -> bool:
    ts = _parse_dt(event.get("ts_utc") or event.get("timestamp") or event.get("created_at_utc"))
    if ts is None:
        return False
    return start_utc <= ts <= end_utc


def _payload(event: dict[str, Any]) -> dict[str, Any]:
    value = event.get("payload")
    return value if isinstance(value, dict) else {}


def _signal_payload(event: dict[str, Any]) -> dict[str, Any]:
    p = _payload(event)
    nested = p.get("payload")
    if isinstance(nested, dict):
        return nested
    return p


def _metadata(event: dict[str, Any]) -> dict[str, Any]:
    p = _signal_payload(event)
    meta = p.get("metadata")
    return meta if isinstance(meta, dict) else {}


def _auction_context(event: dict[str, Any]) -> dict[str, Any]:
    ctx = _metadata(event).get("auction_context")
    return ctx if isinstance(ctx, dict) else {}


def _auction_filters(event: dict[str, Any]) -> dict[str, Any]:
    filters = _metadata(event).get("auction_filters")
    return filters if isinstance(filters, dict) else {}


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _store_symbols(store: dict[str, Any]) -> dict[str, Any]:
    symbols = store.get("symbols")
    return symbols if isinstance(symbols, dict) else {}


def _counter_to_sorted_dict(counter: Counter) -> dict[str, int]:
    return dict(sorted(counter.items(), key=lambda kv: (-kv[1], kv[0])))


@dataclass
class TpoDailyReport:
    report_date: str
    timezone: str
    generated_at_utc: str
    runtime_dir: str
    tpo_store_path: str
    journal_path: str
    battle_telemetry_path: str
    summary: dict[str, Any]
    tpo_store: dict[str, Any]
    journal: dict[str, Any]
    battle_permission: dict[str, Any]
    symbols: dict[str, Any]


class TpoDailyReportBuilder:
    def __init__(
        self,
        *,
        report_date: date,
        timezone_name: str,
        runtime_dir: Path,
        tpo_store_path: Path | None = None,
        journal_path: Path | None = None,
        battle_telemetry_path: Path | None = None,
    ) -> None:
        self.report_date = report_date
        self.timezone_name = timezone_name
        self.runtime_dir = runtime_dir
        self.tpo_store_path = tpo_store_path or runtime_dir / "tpo" / "tpo_latest.json"
        self.journal_path = journal_path or runtime_dir / "radar_journal.ndjson"
        self.battle_telemetry_path = battle_telemetry_path or runtime_dir / "telemetry" / "battle_permission_events.ndjson"
        self.start_utc, self.end_utc = _target_window(report_date, timezone_name)

    def build(self) -> TpoDailyReport:
        store = _read_json(self.tpo_store_path)
        store_symbols = _store_symbols(store)

        store_report = self._build_store_report(store, store_symbols)
        journal_report = self._build_journal_report()
        battle_report = self._build_battle_permission_report()
        summary = self._build_summary(store_report, journal_report, battle_report)
        symbol_report = self._build_symbol_report(store_symbols, journal_report, battle_report)

        return TpoDailyReport(
            report_date=self.report_date.isoformat(),
            timezone=self.timezone_name,
            generated_at_utc=datetime.now(timezone.utc).isoformat(),
            runtime_dir=str(self.runtime_dir),
            tpo_store_path=str(self.tpo_store_path),
            journal_path=str(self.journal_path),
            battle_telemetry_path=str(self.battle_telemetry_path),
            summary=summary,
            tpo_store=store_report,
            journal=journal_report,
            battle_permission=battle_report,
            symbols=symbol_report,
        )

    def _build_store_report(self, store: dict[str, Any], symbols: dict[str, Any]) -> dict[str, Any]:
        market_status_counter: Counter = Counter()
        market_open_counter: Counter = Counter()
        stale_counter: Counter = Counter()
        permission_counter: Counter = Counter()
        modifier_counter: Counter = Counter()
        open_relation_counter: Counter = Counter()
        auction_bias_counter: Counter = Counter()
        session_anchor_counter: Counter = Counter()
        per_symbol: dict[str, Any] = {}

        for symbol, item in symbols.items():
            if not isinstance(item, dict):
                continue

            ctx = item.get("context") if isinstance(item.get("context"), dict) else {}
            filters = item.get("filters") if isinstance(item.get("filters"), dict) else {}

            market_status = ctx.get("market_status") or "UNKNOWN"
            market_is_open = ctx.get("market_is_open")
            market_data_is_stale = ctx.get("market_data_is_stale")
            permission = filters.get("tpo_signal_permission") or "UNKNOWN"
            modifier = filters.get("telegram_modifier") or "UNKNOWN"
            open_relation = ctx.get("open_relation") or filters.get("open_relation") or "UNKNOWN"
            auction_bias = ctx.get("auction_bias") or filters.get("auction_bias") or "UNKNOWN"
            session_anchor = ctx.get("session_anchor") or filters.get("session_anchor") or "UNKNOWN"

            market_status_counter[market_status] += 1
            market_open_counter[str(market_is_open)] += 1
            stale_counter[str(market_data_is_stale)] += 1
            permission_counter[permission] += 1
            modifier_counter[modifier] += 1
            open_relation_counter[open_relation] += 1
            auction_bias_counter[auction_bias] += 1
            session_anchor_counter[session_anchor] += 1

            per_symbol[str(symbol)] = {
                "market_status": market_status,
                "market_is_open": market_is_open,
                "market_data_is_stale": market_data_is_stale,
                "market_data_age_minutes": ctx.get("market_data_age_minutes"),
                "last_bar_timestamp_utc": ctx.get("last_bar_timestamp_utc"),
                "tpo_signal_permission": permission,
                "telegram_modifier": modifier,
                "open_relation": open_relation,
                "auction_bias": auction_bias,
                "session_anchor": session_anchor,
                "session_timezone": ctx.get("session_timezone"),
                "session_open_utc": ctx.get("session_open_utc"),
                "session_open_kyiv": ctx.get("session_open_kyiv"),
                "current_session_id": ctx.get("current_session_id"),
                "previous_session_id": ctx.get("previous_session_id"),
                "current_price": ctx.get("current_price"),
                "current_open": ctx.get("current_open"),
                "nearest_npoc": ctx.get("nearest_npoc"),
                "nearest_npoc_distance": ctx.get("nearest_npoc_distance"),
                "ib_extension_up_pct": ctx.get("ib_extension_up_pct"),
                "ib_extension_down_pct": ctx.get("ib_extension_down_pct"),
                "notes": ctx.get("notes") or [],
                "reasons": filters.get("reasons") or [],
            }

        return {
            "available": bool(store),
            "exporter_version": store.get("exporter_version"),
            "updated_at_utc": store.get("updated_at_utc"),
            "symbols_count": len(symbols),
            "errors_count": len(store.get("errors") or []),
            "errors": store.get("errors") or [],
            "output": store.get("output"),
            "market_status": _counter_to_sorted_dict(market_status_counter),
            "market_is_open": _counter_to_sorted_dict(market_open_counter),
            "market_data_is_stale": _counter_to_sorted_dict(stale_counter),
            "tpo_signal_permission": _counter_to_sorted_dict(permission_counter),
            "telegram_modifier": _counter_to_sorted_dict(modifier_counter),
            "open_relation": _counter_to_sorted_dict(open_relation_counter),
            "auction_bias": _counter_to_sorted_dict(auction_bias_counter),
            "session_anchor": _counter_to_sorted_dict(session_anchor_counter),
            "per_symbol": per_symbol,
        }

    def _build_journal_report(self) -> dict[str, Any]:
        event_type_counter: Counter = Counter()
        symbol_counter: Counter = Counter()
        scenario_counter: Counter = Counter()
        decision_counter: Counter = Counter()
        signal_class_counter: Counter = Counter()
        execution_status_counter: Counter = Counter()
        tpo_permission_counter: Counter = Counter()
        tpo_modifier_counter: Counter = Counter()
        tpo_open_relation_counter: Counter = Counter()
        tpo_auction_bias_counter: Counter = Counter()
        market_closed_symbols: Counter = Counter()
        weekend_skip_symbols: Counter = Counter()
        provider_error_counter: Counter = Counter()
        telegram_counter: Counter = Counter()

        no_symbol_events = 0
        signal_telemetry_events = 0
        signal_candidate_detected_events = 0
        signal_updated_events = 0
        signal_registered_events = 0
        ready_count = 0
        executable_count = 0
        edge_forming_count = 0
        battle_candidate_count = 0
        market_closed_instrument_events = 0
        weekend_skip_fallback_events = 0
        suppressed_by_tpo_downgrade = 0
        stale_context_count = 0
        offline_store_count = 0

        latest_candidates: deque[dict[str, Any]] = deque(maxlen=12)
        sample_provider_errors: list[dict[str, Any]] = []

        for event in _iter_ndjson(self.journal_path):
            if not isinstance(event, dict):
                continue
            if not _event_in_window(event, self.start_utc, self.end_utc):
                continue

            event_type = str(event.get("event_type") or "UNKNOWN")
            event_type_counter[event_type] += 1
            p = _payload(event)
            raw_symbol = event.get("symbol") or p.get("symbol")
            symbol = str(raw_symbol) if raw_symbol not in (None, "") else None

            if symbol:
                symbol_counter[symbol] += 1
            else:
                no_symbol_events += 1

            signal = _signal_payload(event)
            meta = _metadata(event)
            ctx = _auction_context(event)
            filters = _auction_filters(event)

            scenario = _first_non_empty(signal.get("scenario"), p.get("scenario"))
            if scenario:
                scenario_counter[str(scenario)] += 1

            reason = _first_non_empty(p.get("reason"), signal.get("reason"), event.get("reason"))

            if scenario == "MARKET_CLOSED":
                market_closed_instrument_events += 1
                if symbol:
                    market_closed_symbols[symbol] += 1

            if reason == "WEEKEND_MARKET_CLOSED":
                weekend_skip_fallback_events += 1
                if symbol:
                    weekend_skip_symbols[symbol] += 1

            if event_type in {"signal_candidate_detected", "signal_registered", "signal_updated"}:
                signal_telemetry_events += 1
                if event_type == "signal_candidate_detected":
                    signal_candidate_detected_events += 1
                elif event_type == "signal_updated":
                    signal_updated_events += 1
                elif event_type == "signal_registered":
                    signal_registered_events += 1

                decision = signal.get("decision")
                signal_class = signal.get("signal_class")
                execution_status = _first_non_empty(signal.get("execution_status"), meta.get("execution_status"))
                status = signal.get("status")

                if decision:
                    decision_counter[str(decision)] += 1
                if signal_class:
                    signal_class_counter[str(signal_class)] += 1
                if execution_status:
                    execution_status_counter[str(execution_status)] += 1
                if status == "READY":
                    ready_count += 1
                if status == "EDGE_FORMING":
                    edge_forming_count += 1
                if execution_status == "EXECUTABLE":
                    executable_count += 1

                tpo_permission = _first_non_empty(meta.get("tpo_signal_permission"), filters.get("tpo_signal_permission"))
                tpo_modifier = _first_non_empty(meta.get("tpo_telegram_modifier"), filters.get("telegram_modifier"))
                tpo_open_relation = _first_non_empty(meta.get("tpo_open_relation"), ctx.get("open_relation"), filters.get("open_relation"))
                tpo_auction_bias = _first_non_empty(meta.get("tpo_auction_bias"), ctx.get("auction_bias"), filters.get("auction_bias"))

                if tpo_permission:
                    tpo_permission_counter[str(tpo_permission)] += 1
                if tpo_modifier:
                    tpo_modifier_counter[str(tpo_modifier)] += 1
                if tpo_open_relation:
                    tpo_open_relation_counter[str(tpo_open_relation)] += 1
                if tpo_auction_bias:
                    tpo_auction_bias_counter[str(tpo_auction_bias)] += 1

                if tpo_modifier == "DOWNGRADE" or tpo_permission in {
                    "MARKET_CLOSED",
                    "RESEARCH_ONLY",
                    "STALE_DATA",
                    "BLOCKED_BY_CONTEXT",
                }:
                    suppressed_by_tpo_downgrade += 1

                if ctx.get("is_stale") is True or filters.get("is_stale") is True:
                    stale_context_count += 1

                if ctx.get("tpo_source") == "offline_store" or meta.get("auction_telemetry_mode") == "offline_store_read_only":
                    offline_store_count += 1

                is_battle_candidate = (
                    event_type == "signal_candidate_detected"
                    and signal.get("status") == "READY"
                    and execution_status == "EXECUTABLE"
                    and tpo_permission not in {"MARKET_CLOSED", "RESEARCH_ONLY", "STALE_DATA", "BLOCKED_BY_CONTEXT"}
                    and tpo_modifier != "DOWNGRADE"
                )
                if is_battle_candidate:
                    battle_candidate_count += 1

                latest_candidates.append(
                    {
                        "ts_utc": event.get("ts_utc"),
                        "event_type": event_type,
                        "symbol": symbol or "SYSTEM",
                        "scenario": signal.get("scenario"),
                        "decision": signal.get("decision"),
                        "status": signal.get("status"),
                        "signal_class": signal.get("signal_class"),
                        "execution_status": execution_status,
                        "tpo_permission": tpo_permission,
                        "tpo_modifier": tpo_modifier,
                        "open_relation": tpo_open_relation,
                        "auction_bias": tpo_auction_bias,
                    }
                )

            raw = json.dumps(event, ensure_ascii=False)
            if (
                "YFRateLimitError" in raw
                or "Too Many Requests" in raw
                or event.get("status") == "error"
                or "error" in event_type.lower()
            ):
                provider_error_counter[symbol or "SYSTEM"] += 1
                if len(sample_provider_errors) < 10:
                    sample_provider_errors.append(
                        {
                            "ts_utc": event.get("ts_utc"),
                            "event_type": event_type,
                            "symbol": symbol or "SYSTEM",
                            "status": event.get("status"),
                            "snippet": raw[:500],
                        }
                    )

            if "telegram" in event_type.lower():
                telegram_counter[event_type] += 1

        return {
            "window_start_utc": self.start_utc.isoformat(),
            "window_end_utc": self.end_utc.isoformat(),
            "event_types": _counter_to_sorted_dict(event_type_counter),
            "symbols": _counter_to_sorted_dict(symbol_counter),
            "system_no_symbol_events": no_symbol_events,
            "scenarios": _counter_to_sorted_dict(scenario_counter),
            "decisions": _counter_to_sorted_dict(decision_counter),
            "signal_classes": _counter_to_sorted_dict(signal_class_counter),
            "execution_status": _counter_to_sorted_dict(execution_status_counter),
            "tpo_signal_permission": _counter_to_sorted_dict(tpo_permission_counter),
            "tpo_telegram_modifier": _counter_to_sorted_dict(tpo_modifier_counter),
            "tpo_open_relation": _counter_to_sorted_dict(tpo_open_relation_counter),
            "tpo_auction_bias": _counter_to_sorted_dict(tpo_auction_bias_counter),
            "market_closed_symbols": _counter_to_sorted_dict(market_closed_symbols),
            "weekend_skip_symbols": _counter_to_sorted_dict(weekend_skip_symbols),
            "provider_errors": _counter_to_sorted_dict(provider_error_counter),
            "telegram_events": _counter_to_sorted_dict(telegram_counter),
            "counts": {
                "signal_telemetry_events": signal_telemetry_events,
                "signal_candidate_detected_events": signal_candidate_detected_events,
                "signal_updated_events": signal_updated_events,
                "signal_registered_events": signal_registered_events,
                "ready_events": ready_count,
                "executable_events": executable_count,
                "edge_forming_events": edge_forming_count,
                "battle_candidate_events": battle_candidate_count,
                "market_closed_instrument_events": market_closed_instrument_events,
                "weekend_skip_fallback_events": weekend_skip_fallback_events,
                "market_closed_control_events_total": market_closed_instrument_events + weekend_skip_fallback_events,
                "suppressed_by_tpo_downgrade": suppressed_by_tpo_downgrade,
                "stale_context_events": stale_context_count,
                "offline_store_context_events": offline_store_count,
                "system_no_symbol_events": no_symbol_events,
            },
            "samples": {
                "latest_candidates": list(latest_candidates),
                "provider_errors": sample_provider_errors,
            },
        }

    def _build_battle_permission_report(self) -> dict[str, Any]:
        permission_counter: Counter = Counter()
        delivery_counter: Counter = Counter()
        ready_counter: Counter = Counter()
        sent_counter: Counter = Counter()
        blocker_counter: Counter = Counter()
        modifier_counter: Counter = Counter()
        symbol_counter: Counter = Counter()
        scenario_counter: Counter = Counter()
        alignment_counter: Counter = Counter()
        stop_quality_counter: Counter = Counter()
        source_counter: Counter = Counter()

        events_count = 0
        sent_to_telegram_count = 0
        suppressed_count = 0
        battle_ready_count = 0
        latest_events: deque[dict[str, Any]] = deque(maxlen=12)

        for event in _iter_ndjson(self.battle_telemetry_path):
            if not isinstance(event, dict):
                continue
            if not _event_in_window(event, self.start_utc, self.end_utc):
                continue

            events_count += 1
            permission = str(event.get("battle_permission") or "UNKNOWN")
            delivery = str(event.get("telegram_delivery_mode") or "UNKNOWN")
            battle_ready = event.get("battle_ready")
            sent = event.get("sent_to_telegram")
            source = str(event.get("source") or "UNKNOWN")

            permission_counter[permission] += 1
            delivery_counter[delivery] += 1
            ready_counter[str(battle_ready)] += 1
            sent_counter[str(sent)] += 1
            source_counter[source] += 1

            if event.get("symbol"):
                symbol_counter[str(event.get("symbol"))] += 1
            if event.get("scenario"):
                scenario_counter[str(event.get("scenario"))] += 1
            if event.get("signal_alignment"):
                alignment_counter[str(event.get("signal_alignment"))] += 1
            if event.get("stop_quality"):
                stop_quality_counter[str(event.get("stop_quality"))] += 1

            blockers = event.get("battle_permission_blockers")
            if isinstance(blockers, list):
                for blocker in blockers:
                    blocker_counter[str(blocker)] += 1
            elif blockers:
                blocker_counter[str(blockers)] += 1

            modifiers = event.get("battle_permission_modifiers")
            if isinstance(modifiers, list):
                for modifier in modifiers:
                    modifier_counter[str(modifier)] += 1
            elif modifiers:
                modifier_counter[str(modifiers)] += 1

            if sent is True:
                sent_to_telegram_count += 1
            if sent is False:
                suppressed_count += 1
            if battle_ready is True:
                battle_ready_count += 1

            latest_events.append(
                {
                    "ts_utc": event.get("ts_utc"),
                    "symbol": event.get("symbol"),
                    "scenario": event.get("scenario"),
                    "battle_permission": permission,
                    "telegram_delivery_mode": delivery,
                    "sent_to_telegram": sent,
                    "battle_ready": battle_ready,
                    "auction_context_score": event.get("auction_context_score"),
                    "blockers": event.get("battle_permission_blockers") or [],
                    "direction": event.get("direction"),
                    "htf_bias": event.get("htf_bias"),
                    "alignment": event.get("signal_alignment"),
                    "practical_rr": event.get("practical_rr"),
                    "stop_quality": event.get("stop_quality"),
                    "open_relation": event.get("open_relation"),
                    "auction_bias": event.get("auction_bias"),
                }
            )

        return {
            "available": self.battle_telemetry_path.exists(),
            "path": str(self.battle_telemetry_path),
            "events_count": events_count,
            "sent_to_telegram_count": sent_to_telegram_count,
            "suppressed_count": suppressed_count,
            "battle_ready_count": battle_ready_count,
            "battle_permission": _counter_to_sorted_dict(permission_counter),
            "telegram_delivery_mode": _counter_to_sorted_dict(delivery_counter),
            "battle_ready": _counter_to_sorted_dict(ready_counter),
            "sent_to_telegram": _counter_to_sorted_dict(sent_counter),
            "blockers": _counter_to_sorted_dict(blocker_counter),
            "modifiers": _counter_to_sorted_dict(modifier_counter),
            "symbols": _counter_to_sorted_dict(symbol_counter),
            "scenarios": _counter_to_sorted_dict(scenario_counter),
            "signal_alignment": _counter_to_sorted_dict(alignment_counter),
            "stop_quality": _counter_to_sorted_dict(stop_quality_counter),
            "source": _counter_to_sorted_dict(source_counter),
            "samples": {"latest_events": list(latest_events)},
        }

    def _build_summary(
        self,
        store_report: dict[str, Any],
        journal_report: dict[str, Any],
        battle_report: dict[str, Any],
    ) -> dict[str, Any]:
        counts = journal_report.get("counts", {})
        return {
            "status": "ok",
            "store_available": store_report.get("available"),
            "store_symbols_count": store_report.get("symbols_count"),
            "store_errors_count": store_report.get("errors_count"),
            "store_updated_at_utc": store_report.get("updated_at_utc"),
            "journal_signal_telemetry_events": counts.get("signal_telemetry_events", 0),
            "journal_signal_candidate_detected_events": counts.get("signal_candidate_detected_events", 0),
            "journal_signal_updated_events": counts.get("signal_updated_events", 0),
            "journal_battle_candidate_events": counts.get("battle_candidate_events", 0),
            "journal_market_closed_instrument_events": counts.get("market_closed_instrument_events", 0),
            "journal_weekend_skip_fallback_events": counts.get("weekend_skip_fallback_events", 0),
            "journal_market_closed_control_events_total": counts.get("market_closed_control_events_total", 0),
            "journal_suppressed_by_tpo_downgrade": counts.get("suppressed_by_tpo_downgrade", 0),
            "journal_stale_context_events": counts.get("stale_context_events", 0),
            "journal_offline_store_context_events": counts.get("offline_store_context_events", 0),
            "journal_system_no_symbol_events": counts.get("system_no_symbol_events", 0),
            "battle_permission_events": battle_report.get("events_count", 0),
            "battle_ready_events": battle_report.get("battle_ready_count", 0),
            "battle_sent_to_telegram": battle_report.get("sent_to_telegram_count", 0),
            "battle_suppressed": battle_report.get("suppressed_count", 0),
            "market_status_snapshot": store_report.get("market_status", {}),
            "permission_snapshot": store_report.get("tpo_signal_permission", {}),
            "modifier_snapshot": store_report.get("telegram_modifier", {}),
        }

    def _build_symbol_report(
        self,
        store_symbols: dict[str, Any],
        journal_report: dict[str, Any],
        battle_report: dict[str, Any],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        store_per_symbol = self._build_store_report({}, store_symbols).get("per_symbol", {})
        all_symbols = set(store_per_symbol)
        all_symbols.update(journal_report.get("symbols", {}).keys())
        all_symbols.update(battle_report.get("symbols", {}).keys())
        all_symbols.discard("-")
        all_symbols.discard("")
        all_symbols.discard("None")

        for symbol in sorted(all_symbols):
            result[symbol] = {
                "store": store_per_symbol.get(symbol, {}),
                "journal_events": journal_report.get("symbols", {}).get(symbol, 0),
                "market_closed_events": journal_report.get("market_closed_symbols", {}).get(symbol, 0),
                "weekend_skip_events": journal_report.get("weekend_skip_symbols", {}).get(symbol, 0),
                "provider_error_events": journal_report.get("provider_errors", {}).get(symbol, 0),
                "battle_permission_events": battle_report.get("symbols", {}).get(symbol, 0),
            }

        return result


def report_to_markdown(report: TpoDailyReport) -> str:
    data = asdict(report)
    summary = data["summary"]
    store = data["tpo_store"]
    journal = data["journal"]
    battle = data["battle_permission"]
    symbols = data["symbols"]

    lines: list[str] = []
    lines.append(f"# 📊 Daily TPO / Auction Telemetry — {report.report_date}")
    lines.append("")
    lines.append(f"- Timezone: `{report.timezone}`")
    lines.append(f"- Generated UTC: `{report.generated_at_utc}`")
    lines.append(f"- Runtime: `{report.runtime_dir}`")
    lines.append(f"- Battle telemetry: `{report.battle_telemetry_path}`")
    lines.append("")

    lines.append("## Executive Summary")
    lines.append("")
    lines.append(f"- TPO store available: **{summary.get('store_available')}**")
    lines.append(f"- Store symbols: **{summary.get('store_symbols_count')}**")
    lines.append(f"- Store errors: **{summary.get('store_errors_count')}**")
    lines.append(f"- Store updated: `{summary.get('store_updated_at_utc')}`")
    lines.append(f"- Signal telemetry events: **{summary.get('journal_signal_telemetry_events')}**")
    lines.append(f"- Signal candidate detected events: **{summary.get('journal_signal_candidate_detected_events')}**")
    lines.append(f"- Signal updated events: **{summary.get('journal_signal_updated_events')}**")
    lines.append(f"- Battle candidate events: **{summary.get('journal_battle_candidate_events')}**")
    lines.append(f"- Market closed instrument events: **{summary.get('journal_market_closed_instrument_events')}**")
    lines.append(f"- Weekend skip fallback events: **{summary.get('journal_weekend_skip_fallback_events')}**")
    lines.append(f"- Market closed control events total: **{summary.get('journal_market_closed_control_events_total')}**")
    lines.append(f"- Suppressed / downgraded by TPO: **{summary.get('journal_suppressed_by_tpo_downgrade')}**")
    lines.append(f"- Stale context events: **{summary.get('journal_stale_context_events')}**")
    lines.append(f"- Offline store context events: **{summary.get('journal_offline_store_context_events')}**")
    lines.append(f"- System / no-symbol events: **{summary.get('journal_system_no_symbol_events')}**")
    lines.append(f"- Battle permission events: **{summary.get('battle_permission_events')}**")
    lines.append(f"- Battle ready events: **{summary.get('battle_ready_events')}**")
    lines.append(f"- Battle sent to Telegram: **{summary.get('battle_sent_to_telegram')}**")
    lines.append(f"- Battle suppressed: **{summary.get('battle_suppressed')}**")
    lines.append("")

    lines.append("## TPO Store Snapshot")
    lines.append("")
    lines.append("### Market Status")
    for k, v in store.get("market_status", {}).items():
        lines.append(f"- `{k}`: **{v}**")
    lines.append("")
    lines.append("### TPO Permissions")
    for k, v in store.get("tpo_signal_permission", {}).items():
        lines.append(f"- `{k}`: **{v}**")
    lines.append("")
    lines.append("### Telegram Modifiers")
    for k, v in store.get("telegram_modifier", {}).items():
        lines.append(f"- `{k}`: **{v}**")
    lines.append("")
    lines.append("### Open Relation")
    for k, v in store.get("open_relation", {}).items():
        lines.append(f"- `{k}`: **{v}**")
    lines.append("")
    lines.append("### Auction Bias")
    for k, v in store.get("auction_bias", {}).items():
        lines.append(f"- `{k}`: **{v}**")

    lines.append("")
    lines.append("## Battle Permission Telemetry")
    lines.append("")
    lines.append(f"- Available: **{battle.get('available')}**")
    lines.append(f"- Events: **{battle.get('events_count')}**")
    lines.append(f"- Sent to Telegram: **{battle.get('sent_to_telegram_count')}**")
    lines.append(f"- Suppressed: **{battle.get('suppressed_count')}**")
    lines.append(f"- Battle ready: **{battle.get('battle_ready_count')}**")
    lines.append("")
    lines.append("### Battle Permission")
    if battle.get("battle_permission"):
        for k, v in battle.get("battle_permission", {}).items():
            lines.append(f"- `{k}`: **{v}**")
    else:
        lines.append("- None")
    lines.append("")
    lines.append("### Delivery Mode")
    if battle.get("telegram_delivery_mode"):
        for k, v in battle.get("telegram_delivery_mode", {}).items():
            lines.append(f"- `{k}`: **{v}**")
    else:
        lines.append("- None")
    lines.append("")
    lines.append("### Top Blockers")
    if battle.get("blockers"):
        for k, v in battle.get("blockers", {}).items():
            lines.append(f"- `{k}`: **{v}**")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("## Journal Telemetry")
    lines.append("")
    lines.append("### TPO Signal Permission")
    for k, v in journal.get("tpo_signal_permission", {}).items():
        lines.append(f"- `{k}`: **{v}**")
    lines.append("")
    lines.append("### TPO Telegram Modifier")
    for k, v in journal.get("tpo_telegram_modifier", {}).items():
        lines.append(f"- `{k}`: **{v}**")
    lines.append("")
    lines.append("### Market Closed Control")
    lines.append(f"- Instrument MARKET_CLOSED events: **{journal.get('counts', {}).get('market_closed_instrument_events', 0)}**")
    lines.append(f"- Weekend skip fallback events: **{journal.get('counts', {}).get('weekend_skip_fallback_events', 0)}**")
    lines.append(f"- Total closed-control events: **{journal.get('counts', {}).get('market_closed_control_events_total', 0)}**")
    lines.append("")
    lines.append("### Scenarios")
    for k, v in journal.get("scenarios", {}).items():
        lines.append(f"- `{k}`: **{v}**")
    lines.append("")
    lines.append("### Provider Errors")
    provider_errors = journal.get("provider_errors", {})
    if provider_errors:
        for k, v in provider_errors.items():
            lines.append(f"- `{k}`: **{v}**")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("## Symbol Snapshot")
    lines.append("")
    lines.append("| Symbol | Market | Permission | Modifier | Open Relation | Auction Bias | Session | Journal Events | Closed Skips | Battle Gate Events | Provider Errors |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for symbol, item in symbols.items():
        store_item = item.get("store", {})
        lines.append(
            "| "
            f"{symbol} | "
            f"{store_item.get('market_status')} | "
            f"{store_item.get('tpo_signal_permission')} | "
            f"{store_item.get('telegram_modifier')} | "
            f"{store_item.get('open_relation')} | "
            f"{store_item.get('auction_bias')} | "
            f"{store_item.get('session_anchor')} | "
            f"{item.get('journal_events')} | "
            f"{item.get('weekend_skip_events')} | "
            f"{item.get('battle_permission_events')} | "
            f"{item.get('provider_error_events')} |"
        )

    lines.append("")
    lines.append("## Latest Candidate Samples")
    lines.append("")
    samples = journal.get("samples", {}).get("latest_candidates", [])
    if samples:
        for sample in samples:
            lines.append(
                "- "
                f"`{sample.get('ts_utc')}` "
                f"{sample.get('symbol')} "
                f"{sample.get('scenario')} "
                f"status={sample.get('status')} "
                f"exec={sample.get('execution_status')} "
                f"tpo={sample.get('tpo_permission')} "
                f"modifier={sample.get('tpo_modifier')} "
                f"open={sample.get('open_relation')} "
                f"bias={sample.get('auction_bias')}"
            )
    else:
        lines.append("- None")

    lines.append("")
    lines.append("## Latest Battle Permission Events")
    lines.append("")
    battle_samples = battle.get("samples", {}).get("latest_events", [])
    if battle_samples:
        for sample in battle_samples:
            lines.append(
                "- "
                f"`{sample.get('ts_utc')}` "
                f"{sample.get('symbol')} "
                f"{sample.get('scenario')} "
                f"permission={sample.get('battle_permission')} "
                f"delivery={sample.get('telegram_delivery_mode')} "
                f"sent={sample.get('sent_to_telegram')} "
                f"ready={sample.get('battle_ready')} "
                f"score={sample.get('auction_context_score')} "
                f"blockers={sample.get('blockers')} "
                f"dir={sample.get('direction')} "
                f"htf={sample.get('htf_bias')} "
                f"rr={sample.get('practical_rr')} "
                f"stop={sample.get('stop_quality')}"
            )
    else:
        lines.append("- None")

    lines.append("")
    lines.append("## Report Caveats")
    lines.append("")
    lines.append("- Report date uses the selected timezone, so a Kyiv daily report starts at 21:00 UTC on the previous calendar day during UTC+3.")
    lines.append("- Historical events before the latest deployment can contain older TPO fields or older permissions.")
    lines.append("- `Signal telemetry events` includes candidate/update/register events; `Signal candidate detected events` is the cleaner count of newly detected candidates.")
    lines.append("- `Market closed control events total` includes both instrument-level MARKET_CLOSED events and fallback skip events, so it is a control-flow metric, not unique symbols.")
    lines.append("- `System / no-symbol events` are service/worker events and are excluded from Symbol Snapshot.")
    lines.append("- `Battle Permission Telemetry` is written only when a payload reaches TelegramNotifier after battle gate integration.")
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append("- `MARKET_CLOSED` / `MARKET_CLOSED_AND_STALE` має блокувати battle logic.")
    lines.append("- `INSIDE_VA + BALANCE` має бути research/downgrade, а не бойовий Telegram.")
    lines.append("- `OUT_OF_RANGE + DIRECTIONAL_IMBALANCE` має сенс тільки якщо market is open і direction aligned with HTF.")
    lines.append("- `RANGE + RANGE_EXTENSION` допускає оцінку, але не автоматичний battle signal.")
    lines.append("- `BATTLE_READY` — єдиний стан, який має право летіти в Telegram.")
    lines.append("- nPOC залишається interest zone, не entry.")
    return "\n".join(lines)


def write_report(report: TpoDailyReport, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"tpo_daily_report_{report.report_date}.json"
    md_path = output_dir / f"tpo_daily_report_{report.report_date}.md"
    json_path.write_text(json.dumps(_json_safe(asdict(report)), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(report_to_markdown(report), encoding="utf-8")
    return json_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily TPO / auction telemetry report.")
    parser.add_argument("--date", default=None, help="Report date in YYYY-MM-DD. Default: today in selected timezone.")
    parser.add_argument("--timezone", default=os.getenv("DAILY_REPORT_TIMEZONE", DEFAULT_TIMEZONE), help="Report timezone. Default: Europe/Kyiv.")
    parser.add_argument("--runtime-dir", default=None, help="Runtime directory. Default: RUNTIME_DIR/settings.runtime_dir or /var/data/runtime.")
    parser.add_argument("--tpo-store-path", default=None, help="Path to tpo_latest.json.")
    parser.add_argument("--journal-path", default=None, help="Path to radar_journal.ndjson.")
    parser.add_argument("--battle-telemetry-path", default=None, help="Path to battle_permission_events.ndjson.")
    parser.add_argument("--output-dir", default=None, help="Output directory. Default: runtime/reports/tpo.")
    parser.add_argument("--print-markdown", action="store_true", help="Print markdown report to stdout.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tz = ZoneInfo(args.timezone)
    if args.date:
        report_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        report_date = datetime.now(tz).date()

    runtime_dir = Path(args.runtime_dir) if args.runtime_dir else _runtime_dir()
    tpo_store_path = Path(args.tpo_store_path) if args.tpo_store_path else None
    journal_path = Path(args.journal_path) if args.journal_path else None
    battle_telemetry_path = Path(args.battle_telemetry_path) if args.battle_telemetry_path else None
    output_dir = Path(args.output_dir) if args.output_dir else runtime_dir / "reports" / "tpo"

    builder = TpoDailyReportBuilder(
        report_date=report_date,
        timezone_name=args.timezone,
        runtime_dir=runtime_dir,
        tpo_store_path=tpo_store_path,
        journal_path=journal_path,
        battle_telemetry_path=battle_telemetry_path,
    )
    report = builder.build()
    json_path, md_path = write_report(report, output_dir)
    print("[OK] TPO daily report written:")
    print(f"JSON: {json_path}")
    print(f"MD:   {md_path}")
    if args.print_markdown:
        print()
        print(report_to_markdown(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())