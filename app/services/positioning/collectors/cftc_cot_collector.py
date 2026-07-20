from __future__ import annotations

import argparse
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import fmean, pstdev
from typing import Any

import requests

from app.services.positioning.positioning_store import (
    get_positioning_dir,
    read_json_file,
    write_json_atomic,
)


CFTC_COT_COLLECTOR_VERSION = "cftc-cot-collector-v0.1-weekly-official"
DEFAULT_CFTC_BASE_URL = "https://publicreporting.cftc.gov/resource"
DEFAULT_CFTC_SNAPSHOT_FILENAME = "cftc_weekly_positioning_snapshot.json"
TFF_FUTURES_ONLY_DATASET = "gpe5-46if"
DISAGGREGATED_FUTURES_ONLY_DATASET = "72hh-3qpy"

DEFAULT_LOOKBACK_WEEKS = 156
DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_SEC = 0.6
MAX_EXPECTED_REPORT_AGE_DAYS = 10
MIN_GOOD_HISTORY_WEEKS = 52
MIN_USABLE_HISTORY_WEEKS = 26


@dataclass(frozen=True, slots=True)
class CFTCContractSpec:
    symbol: str
    dataset: str
    contract_code: str
    trader_group: str
    long_field: str
    short_field: str
    orientation: int = 1
    market_label: str = ""


CFTC_CONTRACTS: tuple[CFTCContractSpec, ...] = (
    CFTCContractSpec("EURUSD", "TFF", "099741", "LEVERAGED_MONEY", "lev_money_positions_long", "lev_money_positions_short", 1, "Euro FX"),
    CFTCContractSpec("GBPUSD", "TFF", "096742", "LEVERAGED_MONEY", "lev_money_positions_long", "lev_money_positions_short", 1, "British Pound"),
    CFTCContractSpec("USDJPY", "TFF", "097741", "LEVERAGED_MONEY", "lev_money_positions_long", "lev_money_positions_short", -1, "Japanese Yen"),
    CFTCContractSpec("USDCHF", "TFF", "092741", "LEVERAGED_MONEY", "lev_money_positions_long", "lev_money_positions_short", -1, "Swiss Franc"),
    CFTCContractSpec("USDCAD", "TFF", "090741", "LEVERAGED_MONEY", "lev_money_positions_long", "lev_money_positions_short", -1, "Canadian Dollar"),
    CFTCContractSpec("AUDUSD", "TFF", "232741", "LEVERAGED_MONEY", "lev_money_positions_long", "lev_money_positions_short", 1, "Australian Dollar"),
    CFTCContractSpec("NAS100", "TFF", "20974+", "LEVERAGED_MONEY", "lev_money_positions_long", "lev_money_positions_short", 1, "NASDAQ-100 Consolidated"),
    CFTCContractSpec("SPX500", "TFF", "13874+", "LEVERAGED_MONEY", "lev_money_positions_long", "lev_money_positions_short", 1, "S&P 500 Consolidated"),
    CFTCContractSpec("BTCUSD", "TFF", "133741", "LEVERAGED_MONEY", "lev_money_positions_long", "lev_money_positions_short", 1, "CME Bitcoin"),
    CFTCContractSpec("ETHUSD", "TFF", "146021", "LEVERAGED_MONEY", "lev_money_positions_long", "lev_money_positions_short", 1, "CME Ether"),
    CFTCContractSpec("XAUUSD", "DISAGGREGATED", "088691", "MANAGED_MONEY", "m_money_positions_long_all", "m_money_positions_short_all", 1, "Gold COMEX"),
    CFTCContractSpec("UKOIL", "DISAGGREGATED", "06765T", "MANAGED_MONEY", "m_money_positions_long_all", "m_money_positions_short_all", 1, "Brent Last Day NYMEX"),
)


TFF_SELECT_FIELDS = (
    "market_and_exchange_names",
    "report_date_as_yyyy_mm_dd",
    "cftc_contract_market_code",
    "open_interest_all",
    "lev_money_positions_long",
    "lev_money_positions_short",
)

DISAGGREGATED_SELECT_FIELDS = (
    "market_and_exchange_names",
    "report_date_as_yyyy_mm_dd",
    "cftc_contract_market_code",
    "open_interest_all",
    "m_money_positions_long_all",
    "m_money_positions_short_all",
)


def collect_cftc_cot_snapshot(
    *,
    target_date: str | None = None,
    session: Any | None = None,
    base_url: str | None = None,
    lookback_weeks: int | None = None,
    timeout_sec: float | None = None,
    max_attempts: int | None = None,
    backoff_sec: float | None = None,
    contracts: tuple[CFTCContractSpec, ...] = CFTC_CONTRACTS,
) -> dict[str, Any]:
    resolved_date = _parse_date(target_date or date.today().isoformat())
    client = session or requests.Session()
    resolved_base = str(base_url or os.getenv("CFTC_PUBLIC_REPORTING_BASE_URL") or DEFAULT_CFTC_BASE_URL).rstrip("/")
    resolved_lookback = max(8, int(lookback_weeks or _env_int("POSITIONING_CFTC_LOOKBACK_WEEKS", DEFAULT_LOOKBACK_WEEKS)))
    timeout = float(timeout_sec or _env_float("POSITIONING_CFTC_TIMEOUT_SEC", DEFAULT_TIMEOUT_SEC))
    attempts = max(1, int(max_attempts or _env_int("POSITIONING_CFTC_MAX_ATTEMPTS", DEFAULT_MAX_ATTEMPTS)))
    backoff = max(0.0, float(backoff_sec if backoff_sec is not None else _env_float("POSITIONING_CFTC_BACKOFF_SEC", DEFAULT_BACKOFF_SEC)))

    rows_by_dataset: dict[str, list[dict[str, Any]]] = {}
    dataset_health: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []

    for dataset_name, dataset_id, fields in (
        ("TFF", TFF_FUTURES_ONLY_DATASET, TFF_SELECT_FIELDS),
        ("DISAGGREGATED", DISAGGREGATED_FUTURES_ONLY_DATASET, DISAGGREGATED_SELECT_FIELDS),
    ):
        specs = [spec for spec in contracts if spec.dataset == dataset_name]
        if not specs:
            continue
        codes = [spec.contract_code for spec in specs]
        try:
            rows = _fetch_dataset_rows(
                client=client,
                base_url=resolved_base,
                dataset_id=dataset_id,
                select_fields=fields,
                contract_codes=codes,
                timeout_sec=timeout,
                max_attempts=attempts,
                backoff_sec=backoff,
                app_token=os.getenv("CFTC_APP_TOKEN"),
                page_limit=max(1000, len(codes) * (resolved_lookback + 12)),
            )
            rows_by_dataset[dataset_name] = rows
            dataset_health.append({
                "dataset": dataset_name,
                "dataset_id": dataset_id,
                "status": "OK" if rows else "EMPTY",
                "rows": len(rows),
                "contracts_requested": codes,
            })
            if not rows:
                warnings.append(f"cftc_dataset_empty:{dataset_name}")
        except Exception as exc:  # noqa: BLE001
            message = f"cftc_dataset:{dataset_name}:{type(exc).__name__}:{exc}"
            errors.append(message)
            dataset_health.append({
                "dataset": dataset_name,
                "dataset_id": dataset_id,
                "status": "ERROR",
                "rows": 0,
                "contracts_requested": codes,
                "error": message,
            })

    items: list[dict[str, Any]] = []
    symbol_health: list[dict[str, Any]] = []
    for spec in contracts:
        rows = rows_by_dataset.get(spec.dataset) or []
        item = _build_contract_item(
            spec=spec,
            rows=rows,
            target_date=resolved_date,
            lookback_weeks=resolved_lookback,
        )
        if item is None:
            symbol_health.append({
                "symbol": spec.symbol,
                "dataset": spec.dataset,
                "contract_code": spec.contract_code,
                "status": "MISSING",
            })
            warnings.append(f"cftc_contract_missing:{spec.symbol}:{spec.contract_code}")
            continue
        items.append(item)
        symbol_health.append({
            "symbol": spec.symbol,
            "dataset": spec.dataset,
            "contract_code": spec.contract_code,
            "status": item["data_quality"]["status"],
            "report_date": item["report_date"],
            "history_weeks": item["history_weeks"],
        })

    stale_count = sum(1 for item in items if "COT_STALE" in item.get("flags", []))
    if items and len(items) == len(contracts) and not errors and stale_count == 0:
        status = "OK"
    elif items and stale_count == len(items):
        status = "STALE"
    elif items:
        status = "PARTIAL"
    elif errors:
        status = "ERROR"
    else:
        status = "NO_DATA"

    latest_report_date = max((item["report_date"] for item in items), default=None)
    return {
        "version": CFTC_COT_COLLECTOR_VERSION,
        "date": resolved_date.isoformat(),
        "generated_at": _utc_now_iso(),
        "status": status,
        "report_date_latest": latest_report_date,
        "items": items,
        "collector": {
            "name": "cftc_official_cot_collector",
            "mode": "weekly_public_socrata",
            "status": status,
            "base_url": resolved_base,
            "datasets": dataset_health,
            "symbols_requested": [spec.symbol for spec in contracts],
            "symbols_collected": [item["symbol"] for item in items],
            "symbol_health": symbol_health,
            "lookback_weeks": resolved_lookback,
            "errors": errors,
            "warnings": _dedupe(warnings),
            "authentication": "optional_app_token_public_data",
            "report_frequency": "weekly_tuesday_positions",
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }


def collect_and_write_cftc_cot_snapshot(
    *,
    runtime_dir: str | None = None,
    output_path: str | None = None,
    target_date: str | None = None,
    session: Any | None = None,
    persist_empty: bool = False,
    **kwargs: Any,
) -> tuple[Path, dict[str, Any]]:
    path = Path(output_path) if output_path else get_positioning_dir(runtime_dir) / DEFAULT_CFTC_SNAPSHOT_FILENAME
    payload = collect_cftc_cot_snapshot(target_date=target_date, session=session, **kwargs)
    if payload.get("items") or persist_empty:
        write_json_atomic(path, payload)
    return path, payload


def load_cftc_cot_snapshot(path: str | Path) -> dict[str, Any]:
    return read_json_file(Path(path))


def _fetch_dataset_rows(
    *,
    client: Any,
    base_url: str,
    dataset_id: str,
    select_fields: tuple[str, ...],
    contract_codes: list[str],
    timeout_sec: float,
    max_attempts: int,
    backoff_sec: float,
    app_token: str | None,
    page_limit: int,
) -> list[dict[str, Any]]:
    quoted_codes = ",".join(_socrata_quote(code) for code in contract_codes)
    params = {
        "$select": ",".join(select_fields),
        "$where": f"cftc_contract_market_code in ({quoted_codes})",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": str(page_limit),
    }
    headers = {"Accept": "application/json", "User-Agent": "AuctionAlpha-Positioning/1.0"}
    if app_token:
        headers["X-App-Token"] = app_token
    url = f"{base_url}/{dataset_id}.json"

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(url, params=params, headers=headers, timeout=timeout_sec)
            if hasattr(response, "raise_for_status"):
                response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError(f"Expected list from CFTC dataset {dataset_id}")
            return [row for row in payload if isinstance(row, dict)]
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= max_attempts:
                break
            if backoff_sec:
                time.sleep(backoff_sec * attempt)
    assert last_error is not None
    raise last_error


def _build_contract_item(
    *,
    spec: CFTCContractSpec,
    rows: list[dict[str, Any]],
    target_date: date,
    lookback_weeks: int,
) -> dict[str, Any] | None:
    selected: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("cftc_contract_market_code") or "").strip() != spec.contract_code:
            continue
        report_date = _row_date(row)
        if report_date is None or report_date > target_date:
            continue
        long_value = _to_float(row.get(spec.long_field))
        short_value = _to_float(row.get(spec.short_field))
        open_interest = _to_float(row.get("open_interest_all"))
        if long_value is None or short_value is None or not open_interest or open_interest <= 0:
            continue
        selected.append({
            "report_date": report_date,
            "long": long_value,
            "short": short_value,
            "open_interest": open_interest,
            "market_name": str(row.get("market_and_exchange_names") or spec.market_label),
        })

    selected.sort(key=lambda row: row["report_date"])
    selected = selected[-lookback_weeks:]
    if not selected:
        return None

    history: list[dict[str, Any]] = []
    for row in selected:
        raw_net = row["long"] - row["short"]
        oriented_net = raw_net * spec.orientation
        net_pct_oi = oriented_net / row["open_interest"] * 100.0
        history.append({**row, "raw_net": raw_net, "oriented_net": oriented_net, "net_pct_oi": net_pct_oi})

    current = history[-1]
    previous = history[-2] if len(history) >= 2 else None
    weekly_change = current["oriented_net"] - previous["oriented_net"] if previous else None
    weekly_change_pct_oi = current["net_pct_oi"] - previous["net_pct_oi"] if previous else None
    series = [row["net_pct_oi"] for row in history]
    percentile = _percentile_rank(series, current["net_pct_oi"])
    zscore = _zscore(series, current["net_pct_oi"])
    age_days = (target_date - current["report_date"]).days
    primary_tag, interpretation = _classify_cot(
        net_pct_oi=current["net_pct_oi"],
        weekly_change_pct_oi=weekly_change_pct_oi,
        percentile=percentile,
        history_weeks=len(history),
    )

    flags = [
        "CFTC_WEEKLY_COT",
        "CFTC_TFF_FUTURES_ONLY" if spec.dataset == "TFF" else "CFTC_DISAGGREGATED_FUTURES_ONLY",
        "LEVERAGED_MONEY" if spec.dataset == "TFF" else "MANAGED_MONEY",
        "NO_BATTLE_GATE_IMPACT",
    ]
    if spec.orientation < 0:
        flags.append("INVERTED_TO_PROJECT_SYMBOL")
    if len(history) < MIN_USABLE_HISTORY_WEEKS:
        flags.append("COT_HISTORY_SHORT")
    elif len(history) < MIN_GOOD_HISTORY_WEEKS:
        flags.append("COT_HISTORY_LIMITED")
    if age_days > MAX_EXPECTED_REPORT_AGE_DAYS:
        flags.append("COT_STALE")
    if len(history) >= MIN_USABLE_HISTORY_WEEKS and percentile is not None and percentile >= 90:
        flags.append("COT_EXTREME_LONG")
    elif len(history) >= MIN_USABLE_HISTORY_WEEKS and percentile is not None and percentile <= 10:
        flags.append("COT_EXTREME_SHORT")

    if age_days > MAX_EXPECTED_REPORT_AGE_DAYS:
        quality = "STALE"
        confidence = 0.30
    elif len(history) >= MIN_GOOD_HISTORY_WEEKS:
        quality = "GOOD"
        confidence = 0.75
    elif len(history) >= MIN_USABLE_HISTORY_WEEKS:
        quality = "MEDIUM"
        confidence = 0.60
    else:
        quality = "LOW"
        confidence = 0.40

    return {
        "symbol": spec.symbol,
        "report_date": current["report_date"].isoformat(),
        "report_age_days": age_days,
        "dataset": spec.dataset,
        "dataset_id": TFF_FUTURES_ONLY_DATASET if spec.dataset == "TFF" else DISAGGREGATED_FUTURES_ONLY_DATASET,
        "contract_code": spec.contract_code,
        "market_name": current["market_name"],
        "trader_group": spec.trader_group,
        "orientation": "DIRECT" if spec.orientation > 0 else "INVERTED_TO_PROJECT_SYMBOL",
        "positions": {
            "long": round(current["long"], 6),
            "short": round(current["short"], 6),
            "raw_net_contracts": round(current["raw_net"], 6),
            "project_net_contracts": round(current["oriented_net"], 6),
            "weekly_change_net_contracts": _round_optional(weekly_change),
            "open_interest": round(current["open_interest"], 6),
            "net_pct_open_interest": round(current["net_pct_oi"], 6),
            "weekly_change_net_pct_open_interest": _round_optional(weekly_change_pct_oi),
        },
        "normalization": {
            "history_weeks": len(history),
            "lookback_weeks_requested": lookback_weeks,
            "percentile": _round_optional(percentile),
            "zscore": _round_optional(zscore),
            "metric": "project_oriented_net_pct_open_interest",
        },
        "history_weeks": len(history),
        "interpretation": {
            "primary_tag": primary_tag,
            "confidence": confidence,
            "text": interpretation,
            "recommended_usage": "Use as slow weekly context only. Require current TPO/Auction acceptance and LTF confirmation.",
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
        "data_quality": {
            "status": quality,
            "flags": flags,
            "source_lag": "weekly_tuesday_positions_published_after_cutoff",
        },
        "flags": flags,
        "source": f"cftc_public_reporting_{spec.dataset.lower()}_futures_only",
        "source_timestamp": current["report_date"].isoformat(),
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }


def _classify_cot(
    *,
    net_pct_oi: float,
    weekly_change_pct_oi: float | None,
    percentile: float | None,
    history_weeks: int,
) -> tuple[str, str]:
    if history_weeks >= MIN_USABLE_HISTORY_WEEKS and percentile is not None and percentile >= 90:
        return "COT_EXTREME_NET_LONG", "Speculative positioning is near the long extreme of its historical range. This is context, not a standalone short signal."
    if history_weeks >= MIN_USABLE_HISTORY_WEEKS and percentile is not None and percentile <= 10:
        return "COT_EXTREME_NET_SHORT", "Speculative positioning is near the short extreme of its historical range. This is context, not a standalone long signal."
    delta = weekly_change_pct_oi or 0.0
    if net_pct_oi >= 0 and delta >= 0.5:
        return "COT_BUILDING_LONGS", "Speculative net-long exposure increased during the latest reporting week."
    if net_pct_oi >= 0 and delta <= -0.5:
        return "COT_LONG_REDUCTION", "Speculative net-long exposure contracted during the latest reporting week."
    if net_pct_oi < 0 and delta <= -0.5:
        return "COT_BUILDING_SHORTS", "Speculative net-short exposure increased during the latest reporting week."
    if net_pct_oi < 0 and delta >= 0.5:
        return "COT_SHORT_COVERING", "Speculative net-short exposure contracted during the latest reporting week."
    return "COT_NEUTRAL", "Weekly speculative positioning changed only modestly relative to open interest."


def _socrata_quote(value: str) -> str:
    escaped = str(value).replace("'", "''")
    return "'" + escaped + "'"


def _row_date(row: dict[str, Any]) -> date | None:
    value = row.get("report_date_as_yyyy_mm_dd")
    if value is None:
        return None
    text = str(value).strip()[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_date(value: str) -> date:
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def _percentile_rank(values: list[float], current: float) -> float | None:
    finite = [value for value in values if math.isfinite(value)]
    if not finite:
        return None
    less = sum(1 for value in finite if value < current)
    equal = sum(1 for value in finite if value == current)
    return (less + 0.5 * equal) / len(finite) * 100.0


def _zscore(values: list[float], current: float) -> float | None:
    finite = [value for value in values if math.isfinite(value)]
    if len(finite) < 2:
        return None
    sigma = pstdev(finite)
    if sigma == 0:
        return 0.0
    return (current - fmean(finite)) / sigma


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _round_optional(value: float | None) -> float | None:
    return None if value is None else round(value, 6)


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if str(value).strip()))


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        return int(raw) if raw not in {None, ""} else default
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    try:
        return float(raw) if raw not in {None, ""} else default
    except ValueError:
        return default


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect official weekly CFTC COT positioning context.")
    parser.add_argument("--runtime-dir", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--date", default=None, help="Context date YYYY-MM-DD")
    parser.add_argument("--persist-empty", action="store_true")
    args = parser.parse_args()

    path, payload = collect_and_write_cftc_cot_snapshot(
        runtime_dir=args.runtime_dir,
        output_path=args.output,
        target_date=args.date,
        persist_empty=args.persist_empty,
    )
    print(json.dumps({"snapshot": str(path), **payload}, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") not in {"ERROR"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
