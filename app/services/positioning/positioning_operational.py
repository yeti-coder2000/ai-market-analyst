from __future__ import annotations

import copy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .positioning_store import get_positioning_dir, read_json_file, write_json_atomic


POSITIONING_OPERATIONAL_VERSION = "positioning-operational-v0.2-japan-frankfurt-london"
JAPAN_BASELINE_FILENAME = "positioning_operational_japan_baseline.json"
FRANKFURT_BASELINE_FILENAME = "positioning_operational_frankfurt_baseline.json"
DAILY_CLOSE_SNAPSHOT_FILENAME = "positioning_operational_daily_close_latest.json"

JAPAN_BASELINE_REPORT_TYPES = frozenset(
    {"positioning_japan_open", "japan_open_baseline"}
)
FRANKFURT_REPORT_TYPES = frozenset(
    {"morning", "morning_briefing", "morning_combined"}
)
LONDON_1H_REPORT_TYPES = frozenset({"london", "london_1h", "london_open_1h"})
DAILY_CLOSE_REPORT_TYPES = frozenset({"daily_close", "ny_close"})


def apply_operational_positioning_window(
    snapshot: dict[str, Any],
    *,
    runtime_dir: str | None,
    report_date: str,
    report_type: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Build the research-only operational positioning cycle.

    Phases:
    - Japan open: persist a silent absolute price/OI baseline;
    - Frankfurt: calculate Japan-open -> Frankfurt and persist Frankfurt baseline;
    - London +1h: calculate Japan-open -> London +1h plus Frankfurt -> London +1h;
    - NY close: persist the final absolute snapshot for the next Frankfurt report.

    Only copied positioning proxy fields are changed. Battle Gate, signal
    delivery, entries, stops, and TPO telemetry are never touched.
    """

    phase = _phase(report_type)
    source = copy.deepcopy(snapshot) if isinstance(snapshot, dict) else {}
    raw_items = source.get("items")
    items = raw_items if isinstance(raw_items, list) else []
    current = _absolute_items(items)

    positioning_dir = get_positioning_dir(runtime_dir)
    japan_path = positioning_dir / JAPAN_BASELINE_FILENAME
    frankfurt_path = positioning_dir / FRANKFURT_BASELINE_FILENAME
    daily_close_path = positioning_dir / DAILY_CLOSE_SNAPSHOT_FILENAME
    previous_day = _previous_trading_day_summary(daily_close_path, report_date)

    meta: dict[str, Any] = {
        "version": POSITIONING_OPERATIONAL_VERSION,
        "report_date": report_date,
        "report_type": str(report_type or ""),
        "phase": phase,
        "status": "NOT_REQUESTED",
        "japan_baseline_path": str(japan_path),
        "frankfurt_baseline_path": str(frankfurt_path),
        "daily_close_snapshot_path": str(daily_close_path),
        "baseline_timestamp": None,
        "frankfurt_baseline_timestamp": None,
        "current_timestamp": source.get("generated_at"),
        "previous_trading_day": previous_day,
        "symbols": {},
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }

    if phase == "OTHER":
        return source, meta

    if not current:
        meta["status"] = "LIVE_SOURCE_UNAVAILABLE"
        return source, meta

    captured_at = source.get("generated_at") or _utc_now_iso()

    if phase == "JAPAN_OPEN_BASELINE":
        baseline = _baseline_payload(
            report_date=report_date,
            captured_at=captured_at,
            report_type=report_type,
            items=current,
        )
        write_json_atomic(japan_path, baseline)
        meta["status"] = "JAPAN_BASELINE_CAPTURED"
        meta["baseline_timestamp"] = captured_at
        for item in items:
            symbol = _symbol(item)
            if symbol not in current:
                continue
            _mark_absolute_baseline(
                item,
                status="JAPAN_BASELINE_CAPTURED",
                window="japan_open_baseline",
                timestamp=captured_at,
            )
            meta["symbols"][symbol] = item.get("operational_window")
        return source, meta

    if phase == "FRANKFURT_CONTROL":
        frankfurt = _baseline_payload(
            report_date=report_date,
            captured_at=captured_at,
            report_type=report_type,
            items=current,
        )
        write_json_atomic(frankfurt_path, frankfurt)
        meta["frankfurt_baseline_timestamp"] = captured_at

        japan = _valid_baseline(japan_path, report_date)
        if not japan:
            meta["status"] = "JAPAN_BASELINE_MISSING"
            for item in items:
                if _symbol(item) in current:
                    _mark_absolute_baseline(
                        item,
                        status="FRANKFURT_BASELINE_CAPTURED_NO_JAPAN",
                        window="frankfurt_baseline",
                        timestamp=captured_at,
                    )
            return source, meta

        meta["baseline_timestamp"] = japan.get("captured_at")
        ready, missing = _apply_primary_window(
            items,
            current=current,
            baseline_items=japan["items"],
            status="FRANKFURT_DELTA_READY",
            window="japan_open_to_frankfurt",
            baseline_timestamp=japan.get("captured_at"),
            current_timestamp=source.get("generated_at"),
            marker="OPERATIONAL_DELTA_SINCE_JAPAN_OPEN",
            meta_symbols=meta["symbols"],
        )
        meta.update(_coverage_meta(ready, missing, set(current) | set(japan["items"])))
        meta["status"] = (
            "FRANKFURT_DELTA_READY"
            if ready and not missing
            else ("PARTIAL" if ready else "NO_DELTA")
        )
        return source, meta

    if phase == "LONDON_1H_CONTROL":
        japan = _valid_baseline(japan_path, report_date)
        frankfurt = _valid_baseline(frankfurt_path, report_date)
        if not japan and not frankfurt:
            meta["status"] = "BASELINES_MISSING"
            return source, meta

        primary_ready = 0
        missing: set[str] = set()
        if japan:
            meta["baseline_timestamp"] = japan.get("captured_at")
            primary_ready, primary_missing = _apply_primary_window(
                items,
                current=current,
                baseline_items=japan["items"],
                status="LONDON_1H_DELTA_READY",
                window="japan_open_to_london_plus_1h",
                baseline_timestamp=japan.get("captured_at"),
                current_timestamp=source.get("generated_at"),
                marker="OPERATIONAL_DELTA_SINCE_JAPAN_OPEN",
                meta_symbols=meta["symbols"],
            )
            missing.update(primary_missing)
        else:
            missing.update(current)

        frankfurt_ready = 0
        if frankfurt:
            meta["frankfurt_baseline_timestamp"] = frankfurt.get("captured_at")
            frankfurt_ready, frankfurt_missing = _apply_frankfurt_change(
                items,
                current=current,
                baseline_items=frankfurt["items"],
                baseline_timestamp=frankfurt.get("captured_at"),
                current_timestamp=source.get("generated_at"),
            )
            missing.update(frankfurt_missing)
            for item in items:
                symbol = _symbol(item)
                operational = (
                    item.get("operational_window")
                    if isinstance(item.get("operational_window"), dict)
                    else {}
                )
                if symbol and operational:
                    meta["symbols"][symbol] = operational
        else:
            missing.update(current)

        expected = set(current)
        meta.update(_coverage_meta(primary_ready, sorted(missing), expected))
        meta["frankfurt_ready_symbols"] = frankfurt_ready
        if primary_ready == len(expected) and frankfurt_ready == len(expected):
            meta["status"] = "LONDON_1H_DELTA_READY"
        elif primary_ready or frankfurt_ready:
            meta["status"] = "PARTIAL"
        elif not japan:
            meta["status"] = "JAPAN_BASELINE_MISSING"
        else:
            meta["status"] = "NO_DELTA"
        return source, meta

    if phase == "DAILY_CLOSE_SNAPSHOT":
        close_snapshot = _baseline_payload(
            report_date=report_date,
            captured_at=captured_at,
            report_type=report_type,
            items=current,
        )
        close_snapshot["snapshot_role"] = "previous_trading_day_positioning"
        write_json_atomic(daily_close_path, close_snapshot)
        meta["status"] = "DAILY_CLOSE_SNAPSHOT_CAPTURED"
        meta["baseline_timestamp"] = captured_at
        meta["symbols"] = current
        return source, meta

    return source, meta


def _phase(report_type: str | None) -> str:
    normalized = str(report_type or "").strip().lower()
    if normalized in JAPAN_BASELINE_REPORT_TYPES:
        return "JAPAN_OPEN_BASELINE"
    if normalized in FRANKFURT_REPORT_TYPES:
        return "FRANKFURT_CONTROL"
    if normalized in LONDON_1H_REPORT_TYPES:
        return "LONDON_1H_CONTROL"
    if normalized in DAILY_CLOSE_REPORT_TYPES:
        return "DAILY_CLOSE_SNAPSHOT"
    return "OTHER"


def _baseline_payload(
    *,
    report_date: str,
    captured_at: Any,
    report_type: str | None,
    items: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "version": POSITIONING_OPERATIONAL_VERSION,
        "date": report_date,
        "captured_at": captured_at,
        "report_type": str(report_type or ""),
        "items": items,
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }


def _valid_baseline(path: Path, report_date: str) -> dict[str, Any]:
    baseline = _read_baseline(path)
    items = baseline.get("items") if isinstance(baseline.get("items"), dict) else {}
    if str(baseline.get("date") or "") != report_date or not items:
        return {}
    return baseline


def _previous_trading_day_summary(path: Path, report_date: str) -> dict[str, Any]:
    expected_date = _previous_trading_date(report_date)
    close_snapshot = _read_baseline(path)
    items = (
        close_snapshot.get("items")
        if isinstance(close_snapshot.get("items"), dict)
        else {}
    )
    available = str(close_snapshot.get("date") or "") == expected_date and bool(items)
    return {
        "status": "AVAILABLE" if available else "MISSING",
        "expected_date": expected_date,
        "snapshot_date": close_snapshot.get("date") if close_snapshot else None,
        "captured_at": close_snapshot.get("captured_at") if available else None,
        "symbol_count": len(items) if available else 0,
        "symbols": items if available else {},
    }


def _previous_trading_date(value: str) -> str:
    current = date.fromisoformat(value)
    previous = current - timedelta(days=1)
    while previous.weekday() >= 5:
        previous -= timedelta(days=1)
    return previous.isoformat()


def _apply_primary_window(
    items: list[Any],
    *,
    current: dict[str, dict[str, Any]],
    baseline_items: dict[str, Any],
    status: str,
    window: str,
    baseline_timestamp: Any,
    current_timestamp: Any,
    marker: str,
    meta_symbols: dict[str, Any],
) -> tuple[int, list[str]]:
    ready = 0
    missing: list[str] = []
    for item in items:
        symbol = _symbol(item)
        current_row = current.get(symbol)
        baseline_row = baseline_items.get(symbol)
        if not current_row or not isinstance(baseline_row, dict):
            if symbol:
                missing.append(symbol)
            continue
        _apply_delta_to_item(
            item,
            baseline=baseline_row,
            current=current_row,
            status=status,
            window=window,
            baseline_timestamp=baseline_timestamp,
            current_timestamp=current_timestamp,
            marker=marker,
        )
        meta_symbols[symbol] = item.get("operational_window")
        ready += 1

    for symbol in baseline_items:
        if symbol not in current and symbol not in missing:
            missing.append(symbol)
    return ready, sorted(set(missing))


def _apply_frankfurt_change(
    items: list[Any],
    *,
    current: dict[str, dict[str, Any]],
    baseline_items: dict[str, Any],
    baseline_timestamp: Any,
    current_timestamp: Any,
) -> tuple[int, list[str]]:
    ready = 0
    missing: list[str] = []
    for item in items:
        symbol = _symbol(item)
        current_row = current.get(symbol)
        baseline_row = baseline_items.get(symbol)
        if not current_row or not isinstance(baseline_row, dict):
            if symbol:
                missing.append(symbol)
            continue
        operational = (
            item.get("operational_window")
            if isinstance(item.get("operational_window"), dict)
            else {}
        )
        frankfurt_change = _window_payload(
            baseline=baseline_row,
            current=current_row,
            status="FRANKFURT_TO_LONDON_1H_DELTA_READY",
            window="frankfurt_to_london_plus_1h",
            baseline_timestamp=baseline_timestamp,
            current_timestamp=current_timestamp,
        )
        if not operational.get("status"):
            operational.update(
                {
                    "version": POSITIONING_OPERATIONAL_VERSION,
                    "status": "FRANKFURT_ONLY_PARTIAL",
                    "window": "frankfurt_to_london_plus_1h",
                    "baseline_timestamp": baseline_timestamp,
                    "current_timestamp": current_timestamp,
                    "battle_gate_impact": "none",
                    "telegram_signal_impact": "none",
                }
            )
        operational["frankfurt_change"] = frankfurt_change
        item["operational_window"] = operational
        ready += 1

    for symbol in baseline_items:
        if symbol not in current and symbol not in missing:
            missing.append(symbol)
    return ready, sorted(set(missing))


def _coverage_meta(
    ready: int,
    missing: list[str],
    expected_symbols: set[str],
) -> dict[str, Any]:
    return {
        "ready_symbols": ready,
        "expected_symbols": len(expected_symbols),
        "missing_symbols": sorted(set(missing)),
    }


def _absolute_items(items: list[Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        flags = {str(value).upper() for value in item.get("flags") or []}
        if flags.intersection({"STALE_SOURCE_DATA", "MARKET_SUSPENDED"}):
            continue
        symbol = _symbol(item)
        price = _float(item.get("price"))
        open_interest = _float(item.get("open_interest"))
        if not symbol or price is None or price <= 0 or open_interest is None or open_interest <= 0:
            continue
        out[symbol] = {
            "symbol": symbol,
            "price": price,
            "open_interest": open_interest,
            "volume": _float(item.get("volume")),
            "source": item.get("source"),
            "source_timestamp": item.get("source_timestamp"),
        }
    return out


def _mark_absolute_baseline(
    item: dict[str, Any],
    *,
    status: str,
    window: str,
    timestamp: Any,
) -> None:
    item["price_change_pct"] = None
    item["open_interest_change_pct"] = None
    item["perp_open_interest_change_pct"] = None
    flags = [str(value) for value in item.get("flags") or []]
    for marker in ("OPERATIONAL_BASELINE_CAPTURED", status):
        if marker not in flags:
            flags.append(marker)
    item["flags"] = flags
    item["operational_window"] = {
        "version": POSITIONING_OPERATIONAL_VERSION,
        "status": status,
        "window": window,
        "baseline_timestamp": timestamp,
        "current_timestamp": timestamp,
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }


def _apply_delta_to_item(
    item: dict[str, Any],
    *,
    baseline: dict[str, Any],
    current: dict[str, Any],
    status: str,
    window: str,
    baseline_timestamp: Any,
    current_timestamp: Any,
    marker: str,
) -> None:
    payload = _window_payload(
        baseline=baseline,
        current=current,
        status=status,
        window=window,
        baseline_timestamp=baseline_timestamp,
        current_timestamp=current_timestamp,
    )
    price_change = payload.get("price_change_pct")
    oi_change = payload.get("open_interest_change_pct")
    item["price_change_pct"] = price_change
    item["open_interest_change_pct"] = oi_change
    item["perp_open_interest_change_pct"] = oi_change
    flags = [str(value) for value in item.get("flags") or []]
    if marker not in flags:
        flags.append(marker)
    item["flags"] = flags
    item["operational_window"] = payload


def _window_payload(
    *,
    baseline: dict[str, Any],
    current: dict[str, Any],
    status: str,
    window: str,
    baseline_timestamp: Any,
    current_timestamp: Any,
) -> dict[str, Any]:
    return {
        "version": POSITIONING_OPERATIONAL_VERSION,
        "status": status,
        "window": window,
        "baseline_timestamp": baseline_timestamp,
        "current_timestamp": current_timestamp,
        "baseline_price": baseline.get("price"),
        "current_price": current.get("price"),
        "price_change_pct": _pct_change(
            _float(current.get("price")),
            _float(baseline.get("price")),
        ),
        "baseline_open_interest": baseline.get("open_interest"),
        "current_open_interest": current.get("open_interest"),
        "open_interest_change_pct": _pct_change(
            _float(current.get("open_interest")),
            _float(baseline.get("open_interest")),
        ),
        "source": current.get("source"),
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }


def _read_baseline(path: Path) -> dict[str, Any]:
    try:
        payload = read_json_file(path)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _pct_change(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline in {None, 0.0}:
        return None
    return round(((current - baseline) / abs(baseline)) * 100.0, 6)


def _symbol(item: Any) -> str:
    return str(item.get("symbol") or "").strip().upper() if isinstance(item, dict) else ""


def _float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
