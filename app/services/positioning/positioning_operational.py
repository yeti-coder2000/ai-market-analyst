from __future__ import annotations

import copy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .positioning_store import get_positioning_dir, read_json_file, write_json_atomic


POSITIONING_OPERATIONAL_VERSION = "positioning-operational-v0.1-morning-to-london-close"
OPERATIONAL_BASELINE_FILENAME = "positioning_operational_morning_baseline.json"

MORNING_REPORT_TYPES = frozenset({"morning", "morning_briefing", "morning_combined"})
LONDON_CLOSE_REPORT_TYPES = frozenset({"london_close", "london_close_briefing"})


def apply_operational_positioning_window(
    snapshot: dict[str, Any],
    *,
    runtime_dir: str | None,
    report_date: str,
    report_type: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Persist a morning absolute price/OI baseline and calculate London-close delta.

    This function only rewrites research-layer proxy fields in a copied source
    snapshot. It never touches Battle Gate, signal delivery, entries, stops, or
    TPO telemetry.
    """

    phase = _phase(report_type)
    source = copy.deepcopy(snapshot) if isinstance(snapshot, dict) else {}
    raw_items = source.get("items")
    items = raw_items if isinstance(raw_items, list) else []
    current = _absolute_items(items)
    baseline_path = get_positioning_dir(runtime_dir) / OPERATIONAL_BASELINE_FILENAME

    meta: dict[str, Any] = {
        "version": POSITIONING_OPERATIONAL_VERSION,
        "report_date": report_date,
        "report_type": str(report_type or ""),
        "phase": phase,
        "status": "NOT_REQUESTED",
        "baseline_path": str(baseline_path),
        "baseline_timestamp": None,
        "current_timestamp": source.get("generated_at"),
        "symbols": {},
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }

    if phase == "OTHER":
        return source, meta

    if not current:
        meta["status"] = "LIVE_SOURCE_UNAVAILABLE"
        return source, meta

    if phase == "MORNING_BASELINE":
        baseline = {
            "version": POSITIONING_OPERATIONAL_VERSION,
            "date": report_date,
            "captured_at": source.get("generated_at") or _utc_now_iso(),
            "report_type": str(report_type or ""),
            "items": current,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }
        write_json_atomic(baseline_path, baseline)
        meta["status"] = "BASELINE_CAPTURED"
        meta["baseline_timestamp"] = baseline["captured_at"]

        for item in items:
            symbol = _symbol(item)
            base = current.get(symbol)
            if not base:
                continue
            _apply_delta_to_item(
                item,
                baseline=base,
                current=base,
                status="BASELINE_CAPTURED",
                baseline_timestamp=baseline["captured_at"],
                current_timestamp=source.get("generated_at"),
            )
            meta["symbols"][symbol] = item.get("operational_window")
        return source, meta

    baseline = _read_baseline(baseline_path)
    baseline_items = baseline.get("items") if isinstance(baseline.get("items"), dict) else {}
    if str(baseline.get("date") or "") != report_date or not baseline_items:
        meta["status"] = "MORNING_BASELINE_MISSING"
        return source, meta

    meta["baseline_timestamp"] = baseline.get("captured_at")
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
            status="DELTA_READY",
            baseline_timestamp=baseline.get("captured_at"),
            current_timestamp=source.get("generated_at"),
        )
        meta["symbols"][symbol] = item.get("operational_window")
        ready += 1

    for symbol in baseline_items:
        if symbol not in current and symbol not in missing:
            missing.append(symbol)

    expected_symbols = set(current) | set(baseline_items)
    meta["status"] = (
        "DELTA_READY"
        if ready == len(expected_symbols)
        else ("PARTIAL" if ready else "NO_DELTA")
    )
    meta["ready_symbols"] = ready
    meta["missing_symbols"] = sorted(missing)
    return source, meta


def _phase(report_type: str | None) -> str:
    normalized = str(report_type or "").strip().lower()
    if normalized in MORNING_REPORT_TYPES:
        return "MORNING_BASELINE"
    if normalized in LONDON_CLOSE_REPORT_TYPES:
        return "LONDON_CLOSE_DELTA"
    return "OTHER"


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


def _apply_delta_to_item(
    item: dict[str, Any],
    *,
    baseline: dict[str, Any],
    current: dict[str, Any],
    status: str,
    baseline_timestamp: Any,
    current_timestamp: Any,
) -> None:
    price_change = _pct_change(_float(current.get("price")), _float(baseline.get("price")))
    oi_change = _pct_change(
        _float(current.get("open_interest")),
        _float(baseline.get("open_interest")),
    )
    item["price_change_pct"] = price_change
    item["open_interest_change_pct"] = oi_change
    item["perp_open_interest_change_pct"] = oi_change
    flags = [str(value) for value in item.get("flags") or []]
    marker = "OPERATIONAL_BASELINE_CAPTURED" if status == "BASELINE_CAPTURED" else "OPERATIONAL_DELTA_SINCE_MORNING"
    if marker not in flags:
        flags.append(marker)
    item["flags"] = flags
    item["operational_window"] = {
        "version": POSITIONING_OPERATIONAL_VERSION,
        "status": status,
        "window": "morning_baseline_to_london_close",
        "baseline_timestamp": baseline_timestamp,
        "current_timestamp": current_timestamp,
        "baseline_price": baseline.get("price"),
        "current_price": current.get("price"),
        "price_change_pct": price_change,
        "baseline_open_interest": baseline.get("open_interest"),
        "current_open_interest": current.get("open_interest"),
        "open_interest_change_pct": oi_change,
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
