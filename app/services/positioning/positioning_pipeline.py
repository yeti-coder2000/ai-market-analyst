from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

from .collectors.binance_usdm_collector import (
    BINANCE_USDM_SYMBOLS,
    collect_and_write_binance_usdm_snapshot,
)
from .collectors.cftc_cot_collector import (
    CFTC_CONTRACTS,
    DEFAULT_CFTC_SNAPSHOT_FILENAME,
    collect_and_write_cftc_cot_snapshot,
    load_cftc_cot_snapshot,
)
from .collectors.crypto_derivatives_collector import (
    DEFAULT_SNAPSHOT_FILENAME,
    build_crypto_manual_feed_payload,
    load_crypto_snapshot,
)
from .collectors.kraken_futures_collector import (
    DEFAULT_KRAKEN_SNAPSHOT_FILENAME,
    collect_and_write_kraken_futures_snapshot,
)
from .positioning_feed_merger import merge_positioning_feeds, write_merged_feed
from .positioning_operational import apply_operational_positioning_window
from .positioning_service import build_daily_positioning_context
from .positioning_store import (
    append_jsonl,
    get_history_path,
    get_latest_path,
    get_manual_feed_path,
    get_positioning_dir,
    read_json_file,
    save_source_health,
    write_json_atomic,
)


POSITIONING_PIPELINE_VERSION = "positioning-pipeline-v0.5-three-checkpoint-cycle"
CRYPTO_FEED_FILENAME = "crypto_daily_positioning_feed.json"
PIPELINE_HEALTH_FILENAME = "positioning_pipeline_health.json"

LONDON_FOCUS_CFTC_SYMBOLS = frozenset(
    {
        "XAUUSD",
        "EURUSD",
        "GBPUSD",
        "USDJPY",
        "USDCHF",
        "USDCAD",
        "AUDUSD",
        "BTCUSD",
        "ETHUSD",
    }
)
LONDON_FOCUS_CFTC_CONTRACTS = tuple(
    spec for spec in CFTC_CONTRACTS if spec.symbol in LONDON_FOCUS_CFTC_SYMBOLS
)


def refresh_positioning_runtime(
    runtime_dir: str | None = None,
    report_date: str | None = None,
    collect_live_crypto: bool | None = None,
    crypto_session: Any | None = None,
    collect_kraken_fallback: bool | None = None,
    kraken_session: Any | None = None,
    collect_weekly_cot: bool | None = None,
    cot_session: Any | None = None,
    report_type: str | None = None,
) -> dict[str, Any]:
    """
    Refresh Positioning Intelligence runtime artifacts.

    Automatic sources:
    - Binance USD-M public REST for BTCUSD and ETHUSD daily participation;
    - Kraken Futures public tickers as a credential-free live fallback;
    - official CFTC weekly COT for financial, metals, and Brent contracts.

    Optional/fallback sources:
    - last usable crypto derivatives snapshot outside operational reports;
    - manual daily positioning feed, which remains the final duplicate-symbol
      override and must match the report date during operational windows.

    The function is fail-open and research-only. It cannot allow or block a
    signal and cannot modify Battle Gate.
    """

    target_date = str(report_date or date.today().isoformat())
    positioning_dir = get_positioning_dir(runtime_dir)
    positioning_dir.mkdir(parents=True, exist_ok=True)

    manual_feed_path = get_manual_feed_path(runtime_dir=runtime_dir)
    crypto_snapshot_path = positioning_dir / DEFAULT_SNAPSHOT_FILENAME
    crypto_feed_path = positioning_dir / CRYPTO_FEED_FILENAME
    cftc_snapshot_path = positioning_dir / DEFAULT_CFTC_SNAPSHOT_FILENAME
    kraken_snapshot_path = positioning_dir / DEFAULT_KRAKEN_SNAPSHOT_FILENAME
    health_path = positioning_dir / PIPELINE_HEALTH_FILENAME

    feed_paths: list[Path] = []
    sources: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []

    live_enabled = (
        _env_bool("POSITIONING_CRYPTO_LIVE_ENABLED", True)
        if collect_live_crypto is None
        else bool(collect_live_crypto)
    )
    cot_enabled = (
        _env_bool("POSITIONING_CFTC_LIVE_ENABLED", True)
        if collect_weekly_cot is None
        else bool(collect_weekly_cot)
    )
    kraken_fallback_enabled = (
        _env_bool("POSITIONING_KRAKEN_FALLBACK_ENABLED", True)
        if collect_kraken_fallback is None
        else bool(collect_kraken_fallback)
    )
    operational_report = _is_operational_report_type(report_type)

    weekly_cot = _refresh_weekly_cot_snapshot(
        enabled=cot_enabled,
        runtime_dir=runtime_dir,
        snapshot_path=cftc_snapshot_path,
        target_date=target_date,
        session=cot_session,
        sources=sources,
        warnings=warnings,
    )

    live_snapshot = _refresh_live_crypto_snapshot(
        enabled=live_enabled,
        runtime_dir=runtime_dir,
        snapshot_path=crypto_snapshot_path,
        kraken_snapshot_path=kraken_snapshot_path,
        target_date=target_date,
        session=crypto_session,
        kraken_session=kraken_session,
        kraken_fallback_enabled=kraken_fallback_enabled,
        sources=sources,
        errors=errors,
        warnings=warnings,
    )

    if live_snapshot.get("items"):
        operational_source, operational_positioning = apply_operational_positioning_window(
            live_snapshot,
            runtime_dir=runtime_dir,
            report_date=target_date,
            report_type=report_type,
        )
        write_json_atomic(crypto_snapshot_path, operational_source)
    else:
        operational_positioning = {
            "version": "positioning-operational-v0.2-japan-frankfurt-london",
            "report_date": target_date,
            "report_type": str(report_type or ""),
            "phase": "UNAVAILABLE",
            "status": "LIVE_SOURCE_UNAVAILABLE" if live_enabled else "LIVE_SOURCE_DISABLED",
            "symbols": {},
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }

    # Automatic/fallback baseline first. Operational reports never silently
    # reuse a previous runtime snapshot after a failed live refresh. Manual feed
    # remains the final override outside operational reports; during morning or
    # close it must be dated today and live BTC/ETH wins duplicate symbols.
    if live_snapshot.get("items") or not operational_report:
        _prepare_crypto_source(
            snapshot_path=crypto_snapshot_path,
            output_path=crypto_feed_path,
            target_date=target_date,
            feed_paths=feed_paths,
            sources=sources,
            errors=errors,
        )
    else:
        warning = "crypto_snapshot:operational_report_not_reusing_previous_runtime"
        warnings.append(warning)
        sources.append(
            {
                "name": "crypto_snapshot",
                "status": "SKIPPED_NO_LIVE_REFRESH",
                "path": str(crypto_snapshot_path),
                "items": 0,
                "warning": warning,
            }
        )
    _prepare_manual_source(
        path=manual_feed_path,
        feed_paths=feed_paths,
        sources=sources,
        errors=errors,
        warnings=warnings,
        required_date=target_date if operational_report else None,
    )

    crypto_source = _source_by_name(sources, "crypto_snapshot")
    manual_source = _source_by_name(sources, "manual_feed")
    live_source = _source_by_name(sources, "crypto_live_aggregate")

    crypto_items = int((crypto_source or {}).get("items") or 0)
    manual_items = int((manual_source or {}).get("items") or 0)
    live_status = str((live_source or {}).get("status") or "DISABLED")

    if (
        crypto_items >= len(BINANCE_USDM_SYMBOLS)
        and live_status == "OK"
    ):
        status_hint = "OK"
    elif crypto_items or manual_items:
        status_hint = "STALE" if live_status == "ERROR" and crypto_items else "PARTIAL"
    elif errors:
        status_hint = "ERROR"
    else:
        status_hint = "NO_DATA"

    if not manual_feed_path.exists():
        warnings.append(f"missing_optional_source:{manual_feed_path}")
    if not crypto_snapshot_path.exists():
        warnings.append(f"missing_optional_source:{crypto_snapshot_path}")

    usable_sources = [
        source
        for source in sources
        if source.get("name") in {"crypto_snapshot", "manual_feed"}
        and int(source.get("items") or 0) > 0
    ]
    available_sources = [
        source
        for source in sources
        if source.get("status") in {"OK", "PARTIAL", "EMPTY", "STALE"}
    ]

    merged = merge_positioning_feeds(
        feed_paths=feed_paths,
        target_date=target_date,
        dedupe_by_symbol=True,
        source_priority=[str(crypto_feed_path)] if operational_report else None,
    )
    merged["pipeline_meta"] = {
        "version": POSITIONING_PIPELINE_VERSION,
        "report_date": target_date,
        "runtime_dir": str(positioning_dir.parent),
        "status_hint": status_hint,
        "live_crypto_enabled": live_enabled,
        "kraken_fallback_enabled": kraken_fallback_enabled,
        "operational_positioning": operational_positioning,
        "weekly_cot_enabled": cot_enabled,
        "weekly_cot_status": weekly_cot.get("status"),
        "weekly_cot_items": len(weekly_cot.get("items") or []),
        "sources_expected": ["binance_usdm_live", "kraken_futures_live", "cftc_cot_live"],
        "sources_optional": ["manual_feed"],
        "sources_available": [str(source.get("name")) for source in available_sources],
        "sources_usable": [str(source.get("name")) for source in usable_sources],
        "sources": sources,
        "errors": errors,
        "warnings": warnings,
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }

    merged_path = write_merged_feed(merged, runtime_dir=runtime_dir)
    snapshot = build_daily_positioning_context(
        runtime_dir=runtime_dir,
        feed_path=str(merged_path),
        persist=False,
        fallback_date=target_date,
    )
    snapshot["weekly_cot"] = weekly_cot
    snapshot["operational_positioning"] = operational_positioning
    _persist_enriched_snapshot(snapshot=snapshot, runtime_dir=runtime_dir)

    result = {
        "version": POSITIONING_PIPELINE_VERSION,
        "ok": str(snapshot.get("status") or "") not in {"ERROR"},
        "status": snapshot.get("status"),
        "report_date": target_date,
        "runtime_dir": str(positioning_dir.parent),
        "positioning_dir": str(positioning_dir),
        "merged_feed": str(merged_path),
        "latest_snapshot": str(positioning_dir / "daily_positioning_latest.json"),
        "live_crypto_enabled": live_enabled,
        "kraken_fallback_enabled": kraken_fallback_enabled,
        "operational_positioning_status": operational_positioning.get("status"),
        "weekly_cot_enabled": cot_enabled,
        "weekly_cot_status": weekly_cot.get("status"),
        "weekly_cot_items": len(weekly_cot.get("items") or []),
        "sources": sources,
        "errors": errors,
        "warnings": warnings,
        "items": len(snapshot.get("items") or []),
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }
    write_json_atomic(health_path, result)
    return result


def _refresh_weekly_cot_snapshot(
    *,
    enabled: bool,
    runtime_dir: str | None,
    snapshot_path: Path,
    target_date: str,
    session: Any | None,
    sources: list[dict[str, Any]],
    warnings: list[str],
) -> dict[str, Any]:
    if not enabled:
        payload = {
            "version": "cftc-cot-disabled",
            "date": target_date,
            "status": "DISABLED",
            "items": [],
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }
        sources.append({
            "name": "cftc_cot_live",
            "status": "DISABLED",
            "path": str(snapshot_path),
            "items": 0,
        })
        return payload

    try:
        path, payload = collect_and_write_cftc_cot_snapshot(
            runtime_dir=runtime_dir,
            output_path=str(snapshot_path),
            target_date=target_date,
            session=session,
            persist_empty=False,
            contracts=(
                CFTC_CONTRACTS
                if _env_bool("POSITIONING_INCLUDE_INACTIVE_US_COT", False)
                else LONDON_FOCUS_CFTC_CONTRACTS
            ),
        )
        collector = payload.get("collector") if isinstance(payload.get("collector"), dict) else {}
        status = str(payload.get("status") or collector.get("status") or "NO_DATA")
        item_count = len(payload.get("items") or [])
        collector_errors = [str(value) for value in collector.get("errors") or []]
        collector_warnings = [str(value) for value in collector.get("warnings") or []]

        if item_count:
            warnings.extend(f"cftc_cot_live:{value}" for value in collector_warnings)
            warnings.extend(f"cftc_cot_live:{value}" for value in collector_errors)
            sources.append({
                "name": "cftc_cot_live",
                "status": status,
                "path": str(path),
                "items": item_count,
                "report_date_latest": payload.get("report_date_latest"),
                "symbols_collected": collector.get("symbols_collected") or [],
                "errors": collector_errors,
                "warnings": collector_warnings,
                "battle_gate_impact": "none",
                "telegram_signal_impact": "none",
            })
            return payload

        if snapshot_path.exists():
            fallback = load_cftc_cot_snapshot(snapshot_path)
            fallback = dict(fallback)
            fallback["status"] = "STALE"
            fallback["runtime_fallback"] = {
                "reason": "live_cftc_refresh_unavailable",
                "target_date": target_date,
                "live_errors": collector_errors,
            }
            warnings.append("cftc_cot_live:using_last_persisted_snapshot")
            warnings.extend(f"cftc_cot_live:{value}" for value in collector_errors)
            sources.append({
                "name": "cftc_cot_live",
                "status": "STALE",
                "path": str(snapshot_path),
                "items": len(fallback.get("items") or []),
                "fallback": True,
                "errors": collector_errors,
                "battle_gate_impact": "none",
                "telegram_signal_impact": "none",
            })
            return fallback

        warnings.extend(f"cftc_cot_live:{value}" for value in collector_errors)
        sources.append({
            "name": "cftc_cot_live",
            "status": status,
            "path": str(snapshot_path),
            "items": 0,
            "errors": collector_errors,
            "warnings": collector_warnings,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        })
        return payload
    except Exception as exc:  # noqa: BLE001
        message = f"cftc_cot_live:{type(exc).__name__}:{exc}"
        warnings.append(message)
        if snapshot_path.exists():
            fallback = dict(load_cftc_cot_snapshot(snapshot_path))
            fallback["status"] = "STALE"
            fallback["runtime_fallback"] = {
                "reason": "live_cftc_refresh_exception",
                "target_date": target_date,
                "live_errors": [message],
            }
            sources.append({
                "name": "cftc_cot_live",
                "status": "STALE",
                "path": str(snapshot_path),
                "items": len(fallback.get("items") or []),
                "fallback": True,
                "error": message,
                "battle_gate_impact": "none",
                "telegram_signal_impact": "none",
            })
            return fallback
        sources.append({
            "name": "cftc_cot_live",
            "status": "ERROR",
            "path": str(snapshot_path),
            "items": 0,
            "error": message,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        })
        return {
            "version": "cftc-cot-runtime-error",
            "date": target_date,
            "status": "ERROR",
            "items": [],
            "errors": [message],
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }


def _persist_enriched_snapshot(
    *,
    snapshot: dict[str, Any],
    runtime_dir: str | None,
) -> None:
    source_health = snapshot.get("source_health")
    if isinstance(source_health, dict):
        save_source_health(source_health, runtime_dir)
    write_json_atomic(get_latest_path(runtime_dir), snapshot)
    append_jsonl(get_history_path(runtime_dir), snapshot)


def _refresh_live_crypto_snapshot(
    *,
    enabled: bool,
    runtime_dir: str | None,
    snapshot_path: Path,
    kraken_snapshot_path: Path,
    target_date: str,
    session: Any | None,
    kraken_session: Any | None,
    kraken_fallback_enabled: bool,
    sources: list[dict[str, Any]],
    errors: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    if not enabled:
        sources.append(
            {
                "name": "binance_usdm_live",
                "status": "DISABLED",
                "path": str(snapshot_path),
                "items": 0,
            }
        )
        sources.append(
            {
                "name": "kraken_futures_live",
                "status": "DISABLED",
                "path": str(kraken_snapshot_path),
                "items": 0,
            }
        )
        sources.append(
            {
                "name": "crypto_live_aggregate",
                "status": "DISABLED",
                "path": str(snapshot_path),
                "items": 0,
            }
        )
        return {}

    combined: dict[str, dict[str, Any]] = {}
    provider_versions: list[str] = []
    live_errors: list[str] = []
    live_warnings: list[str] = []

    try:
        path, payload = collect_and_write_binance_usdm_snapshot(
            runtime_dir=runtime_dir,
            output_path=str(snapshot_path),
            target_date=target_date,
            session=session,
            persist_empty=False,
        )
        collector = payload.get("collector") if isinstance(payload.get("collector"), dict) else {}
        item_count = len(payload.get("items") or [])
        status = str(collector.get("status") or ("OK" if item_count else "ERROR"))
        collector_errors = [str(value) for value in collector.get("errors") or []]
        collector_warnings = [str(value) for value in collector.get("warnings") or []]
        provider_versions.append(str(payload.get("version") or "binance_unknown"))
        for item in payload.get("items") or []:
            if isinstance(item, dict) and item.get("symbol"):
                combined[str(item["symbol"]).upper()] = dict(item)
        live_errors.extend(f"binance_usdm_live:{value}" for value in collector_errors)
        live_warnings.extend(f"binance_usdm_live:{value}" for value in collector_warnings)

        sources.append(
            {
                "name": "binance_usdm_live",
                "status": status,
                "path": str(path),
                "items": item_count,
                "snapshot_written": bool(item_count),
                "symbols_requested": collector.get("symbols_requested") or [],
                "symbols_collected": collector.get("symbols_collected") or [],
                "errors": collector_errors,
                "warnings": collector_warnings,
                "battle_gate_impact": "none",
                "telegram_signal_impact": "none",
            }
        )
    except Exception as exc:  # noqa: BLE001
        message = f"binance_usdm_live:{type(exc).__name__}:{exc}"
        live_errors.append(message)
        sources.append(
            {
                "name": "binance_usdm_live",
                "status": "ERROR",
                "path": str(snapshot_path),
                "items": 0,
                "snapshot_written": False,
                "error": message,
                "battle_gate_impact": "none",
                "telegram_signal_impact": "none",
            }
        )

    missing = [symbol for symbol in BINANCE_USDM_SYMBOLS if symbol not in combined]
    if missing and kraken_fallback_enabled:
        try:
            path, payload = collect_and_write_kraken_futures_snapshot(
                runtime_dir=runtime_dir,
                output_path=str(kraken_snapshot_path),
                target_date=target_date,
                symbols=missing,
                session=kraken_session,
                persist_empty=False,
            )
            collector = payload.get("collector") if isinstance(payload.get("collector"), dict) else {}
            status = str(collector.get("status") or "ERROR")
            provider_versions.append(str(payload.get("version") or "kraken_unknown"))
            for item in payload.get("items") or []:
                if isinstance(item, dict) and item.get("symbol"):
                    combined.setdefault(str(item["symbol"]).upper(), dict(item))
            collector_errors = [str(value) for value in collector.get("errors") or []]
            collector_warnings = [str(value) for value in collector.get("warnings") or []]
            live_errors.extend(f"kraken_futures_live:{value}" for value in collector_errors)
            live_warnings.extend(f"kraken_futures_live:{value}" for value in collector_warnings)
            sources.append(
                {
                    "name": "kraken_futures_live",
                    "status": status,
                    "path": str(path),
                    "items": len(payload.get("items") or []),
                    "snapshot_written": bool(payload.get("items")),
                    "symbols_requested": missing,
                    "symbols_collected": collector.get("symbols_collected") or [],
                    "errors": collector_errors,
                    "warnings": collector_warnings,
                    "fallback_for": "binance_usdm_live",
                    "battle_gate_impact": "none",
                    "telegram_signal_impact": "none",
                }
            )
        except Exception as exc:  # noqa: BLE001
            message = f"kraken_futures_live:{type(exc).__name__}:{exc}"
            live_errors.append(message)
            sources.append(
                {
                    "name": "kraken_futures_live",
                    "status": "ERROR",
                    "path": str(kraken_snapshot_path),
                    "items": 0,
                    "error": message,
                    "fallback_for": "binance_usdm_live",
                    "battle_gate_impact": "none",
                    "telegram_signal_impact": "none",
                }
            )
    else:
        sources.append(
            {
                "name": "kraken_futures_live",
                "status": "NOT_NEEDED" if not missing else "DISABLED",
                "path": str(kraken_snapshot_path),
                "items": 0,
                "fallback_for": "binance_usdm_live",
                "battle_gate_impact": "none",
                "telegram_signal_impact": "none",
            }
        )

    items = [combined[symbol] for symbol in BINANCE_USDM_SYMBOLS if symbol in combined]
    stale_items = [
        item
        for item in items
        if "STALE_SOURCE_DATA" in {str(value).upper() for value in item.get("flags") or []}
    ]
    aggregate_status = (
        "OK"
        if len(items) == len(BINANCE_USDM_SYMBOLS) and not stale_items
        else ("PARTIAL" if items else "ERROR")
    )
    if items:
        warnings.extend(live_errors)
        warnings.extend(live_warnings)
    else:
        errors.extend(live_errors)
        warnings.extend(live_warnings)

    aggregate = {
        "version": "crypto-live-aggregate-v0.1-provider-fallback",
        "date": target_date,
        "generated_at": next(
            (str(item.get("source_timestamp")) for item in items if item.get("source_timestamp")),
            None,
        ),
        "items": items,
        "collector": {
            "name": "crypto_live_provider_aggregate",
            "mode": "binance_primary_kraken_fallback",
            "status": aggregate_status,
            "provider_versions": provider_versions,
            "symbols_requested": list(BINANCE_USDM_SYMBOLS),
            "symbols_collected": [item.get("symbol") for item in items],
            "stale_symbols": [item.get("symbol") for item in stale_items],
            "errors": live_errors,
            "warnings": live_warnings,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }
    if items:
        write_json_atomic(snapshot_path, aggregate)

    sources.append(
        {
            "name": "crypto_live_aggregate",
            "status": aggregate_status,
            "path": str(snapshot_path),
            "items": len(items),
            "symbols_collected": [item.get("symbol") for item in items],
            "errors": live_errors,
            "warnings": live_warnings,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }
    )
    return aggregate


def _prepare_crypto_source(
    *,
    snapshot_path: Path,
    output_path: Path,
    target_date: str,
    feed_paths: list[Path],
    sources: list[dict[str, Any]],
    errors: list[str],
) -> None:
    if not snapshot_path.exists():
        sources.append(
            {
                "name": "crypto_snapshot",
                "status": "MISSING",
                "path": str(snapshot_path),
                "items": 0,
            }
        )
        return

    try:
        snapshot = load_crypto_snapshot(snapshot_path)
        payload = build_crypto_manual_feed_payload(snapshot, target_date=target_date)
        _enrich_crypto_feed_payload(payload=payload, snapshot=snapshot)
        write_json_atomic(output_path, payload)
        item_count = len(payload.get("items") or [])
        feed_paths.append(output_path)
        collector = snapshot.get("collector") if isinstance(snapshot.get("collector"), dict) else {}
        collector_status = str(collector.get("status") or "").upper()
        snapshot_status = (
            collector_status
            if collector_status in {"OK", "PARTIAL", "STALE"}
            else ("OK" if item_count else "EMPTY")
        )
        sources.append(
            {
                "name": "crypto_snapshot",
                "status": snapshot_status,
                "path": str(snapshot_path),
                "feed_path": str(output_path),
                "source_date": snapshot.get("date"),
                "generated_at": snapshot.get("generated_at"),
                "collector": collector.get("name"),
                "items": item_count,
            }
        )
    except Exception as exc:  # noqa: BLE001
        message = f"crypto_snapshot:{type(exc).__name__}:{exc}"
        errors.append(message)
        sources.append(
            {
                "name": "crypto_snapshot",
                "status": "ERROR",
                "path": str(snapshot_path),
                "items": 0,
                "error": message,
            }
        )



def _enrich_crypto_feed_payload(
    *,
    payload: dict[str, Any],
    snapshot: dict[str, Any],
) -> None:
    """Preserve absolute audit fields and honest upstream collector metadata."""

    raw_items = snapshot.get("items") or []
    raw_by_symbol: dict[str, dict[str, Any]] = {}
    if isinstance(raw_items, list):
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            symbol = _canonical_crypto_symbol(raw.get("symbol"))
            if symbol:
                raw_by_symbol[symbol] = raw

    feed_items = payload.get("items") or []
    if isinstance(feed_items, list):
        for item in feed_items:
            if not isinstance(item, dict):
                continue
            raw = raw_by_symbol.get(_canonical_crypto_symbol(item.get("symbol"))) or {}
            for field in ("price", "volume", "open_interest"):
                if raw.get(field) is not None:
                    item[field] = raw.get(field)
            if isinstance(raw.get("operational_window"), dict):
                item["operational_window"] = dict(raw["operational_window"])

    upstream = snapshot.get("collector") if isinstance(snapshot.get("collector"), dict) else {}
    payload["collector"] = {
        "name": "crypto_derivatives_positioning_adapter",
        "mode": upstream.get("mode") or "offline_snapshot",
        "status": upstream.get("status"),
        "upstream_name": upstream.get("name"),
        "upstream_version": snapshot.get("version"),
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }


def _canonical_crypto_symbol(value: Any) -> str:
    raw = str(value or "").strip().upper().replace("-", "").replace("/", "")
    if raw in {"BTC", "BTCUSD", "BTCUSDT"}:
        return "BTCUSD"
    if raw in {"ETH", "ETHUSD", "ETHUSDT"}:
        return "ETHUSD"
    return raw

def _prepare_manual_source(
    *,
    path: Path,
    feed_paths: list[Path],
    sources: list[dict[str, Any]],
    errors: list[str],
    warnings: list[str],
    required_date: str | None = None,
) -> None:
    if not path.exists():
        sources.append(
            {
                "name": "manual_feed",
                "status": "MISSING",
                "path": str(path),
                "items": 0,
            }
        )
        return

    try:
        payload = read_json_file(path)
        items = payload.get("items") or []
        if not isinstance(items, list):
            raise ValueError("manual feed items must be a list")
        source_date = str(payload.get("date") or "").strip()
        if required_date and source_date != required_date:
            warning = (
                "manual_feed:source_date_mismatch:"
                f"expected={required_date}:actual={source_date or 'missing'}"
            )
            warnings.append(warning)
            sources.append(
                {
                    "name": "manual_feed",
                    "status": "STALE",
                    "path": str(path),
                    "source_date": source_date or None,
                    "items": 0,
                    "excluded_from_operational_report": True,
                    "warning": warning,
                }
            )
            return
        feed_paths.append(path)
        sources.append(
            {
                "name": "manual_feed",
                "status": "OK" if items else "EMPTY",
                "path": str(path),
                "source_date": payload.get("date"),
                "items": len(items),
            }
        )
    except Exception as exc:  # noqa: BLE001
        message = f"manual_feed:{type(exc).__name__}:{exc}"
        errors.append(message)
        sources.append(
            {
                "name": "manual_feed",
                "status": "ERROR",
                "path": str(path),
                "items": 0,
                "error": message,
            }
        )


def _source_by_name(
    sources: list[dict[str, Any]],
    name: str,
) -> dict[str, Any] | None:
    for source in sources:
        if source.get("name") == name:
            return source
    return None


def _is_operational_report_type(report_type: str | None) -> bool:
    return str(report_type or "").strip().lower() in {
        "positioning_japan_open",
        "japan_open_baseline",
        "morning",
        "morning_briefing",
        "morning_combined",
        "london",
        "london_1h",
        "london_open_1h",
        "london_close",
        "london_close_briefing",
        "daily_close",
        "ny_close",
    }


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Positioning Intelligence runtime artifacts.")
    parser.add_argument("--runtime-dir", default=None, help="Runtime directory.")
    parser.add_argument("--date", default=None, help="Report date YYYY-MM-DD.")
    parser.add_argument(
        "--report-type",
        default=None,
        help="positioning_japan_open, morning_combined, london_1h, or daily_close operational phase.",
    )
    parser.add_argument(
        "--no-live-crypto",
        action="store_true",
        help="Disable Binance live collection and use only local fallback sources.",
    )
    parser.add_argument(
        "--no-live-cot",
        action="store_true",
        help="Disable official CFTC weekly collection and use only persisted fallback.",
    )
    args = parser.parse_args()

    result = refresh_positioning_runtime(
        runtime_dir=args.runtime_dir,
        report_date=args.date,
        report_type=args.report_type,
        collect_live_crypto=not args.no_live_crypto,
        collect_weekly_cot=not args.no_live_cot,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") != "ERROR" else 1


if __name__ == "__main__":
    raise SystemExit(main())
