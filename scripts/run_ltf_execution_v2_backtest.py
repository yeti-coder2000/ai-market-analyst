from __future__ import annotations

"""Fetch M5 history, replay LTF Execution v2 and build the OTD/ORR census.

The command is research-only.  It writes isolated, versioned artifacts under
``/var/data/runtime/research`` by default and never mutates production cache,
signal state, Battle Gate state, or Telegram delivery state.

Backtest Integrity v2.0 measures auction-event development independently from
the LTF entry model, then reports both denominators side by side.

Twelve Data history is paged backwards from the newest closed bar until the
provider's ``earliest_timestamp`` boundary.  Yahoo-backed GER40 is requested
for its documented maximum 60-day intraday window.
"""

import argparse
from datetime import UTC, datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
from typing import Any, Mapping, Sequence

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.instrument_batches import INSTRUMENT_BATCHES
from app.services.ltf_execution_backtest import (
    ExecutionCostModel,
    HistoricalWatchCandidate,
    compile_backtest_report,
    normalize_m5_history,
    reconstruct_tpo_watch_candidates,
    replay_candidate,
)
from app.services.otd_orr_event_census import (
    DEFAULT_PRIMARY_DEVELOPMENT_R,
    measure_event_development,
)


RUNNER_VERSION = (
    "ltf-execution-v2-provider-depth-runner-v2.0-otd-orr-event-census"
)
DEFAULT_OUTPUT_ROOT = Path(
    os.getenv(
        "LTF_V2_BACKTEST_OUTPUT_ROOT",
        "/var/data/runtime/research/ltf_v2_backtest",
    )
)
DEFAULT_TWELVEDATA_OUTPUTSIZE = 5000
DEFAULT_PROVIDER_PAUSE_SECONDS = 1.6
DEFAULT_TWELVEDATA_LOOKBACK_DAYS = 730
DEFAULT_CFTC_LOOKBACK_WEEKS = 156
DEFAULT_OPERATIONAL_HISTORY_PATH = Path(
    os.getenv(
        "POSITIONING_HISTORY_PATH",
        "/var/data/runtime/positioning/daily_positioning_history.jsonl",
    )
)
MAX_PROVIDER_PAGES_PER_SYMBOL = 5000

TWELVEDATA_SYMBOLS: dict[str, str] = {
    "XAUUSD": "XAU/USD",
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "USDCHF": "USD/CHF",
    "USDCAD": "USD/CAD",
    "AUDUSD": "AUD/USD",
    "BTCUSD": "BTC/USD",
    "ETHUSD": "ETH/USD",
}
YFINANCE_SYMBOLS: dict[str, tuple[str, ...]] = {
    "GER40": ("^GDAXI", "EXS1.DE", "DAX"),
    "NAS100": ("^NDX",),
    "SPX500": ("^GSPC",),
    "UKOIL": ("BZ=F",),
}

SENSITIVE_KEY_FRAGMENTS = (
    "api_key",
    "apikey",
    "authorization",
    "password",
    "secret",
    "token",
)


class ProviderDepthBacktestError(RuntimeError):
    """Fail-closed error for provider-depth research runs."""


def _utc(value: Any) -> datetime:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ProviderDepthBacktestError("provider returned an invalid timestamp")
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.to_pydatetime()


def _iso(value: Any) -> str:
    return _utc(value).isoformat()


def _closed_m5_now() -> datetime:
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    minute = now.minute - (now.minute % 5)
    return now.replace(minute=minute) - timedelta(minutes=5)


def _active_symbols() -> list[str]:
    ordered: list[str] = []
    for group in ("core", "fx_major", "indices"):
        for symbol in INSTRUMENT_BATCHES[group]["symbols"]:
            normalized = str(symbol).upper()
            if normalized not in ordered:
                ordered.append(normalized)
    for symbol in (*TWELVEDATA_SYMBOLS, *YFINANCE_SYMBOLS):
        if symbol not in ordered:
            ordered.append(symbol)
    return ordered


def _safe_json_error(payload: Any) -> tuple[Any, Any]:
    if not isinstance(payload, Mapping):
        return None, None
    return payload.get("code"), payload.get("message")


def _request_json(
    endpoint: str,
    *,
    params: Mapping[str, Any],
    timeout_seconds: int = 45,
    max_attempts: int = 6,
) -> dict[str, Any]:
    """Call Twelve Data without ever interpolating a credential into logs."""

    try:
        import requests
    except ImportError as error:
        raise ProviderDepthBacktestError(
            "requests dependency is unavailable"
        ) from error

    last_status: int | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(
                f"https://api.twelvedata.com/{endpoint}",
                params=dict(params),
                timeout=timeout_seconds,
            )
            last_status = int(response.status_code)
            payload = response.json()
        except Exception as error:  # noqa: BLE001
            if attempt >= max_attempts:
                raise ProviderDepthBacktestError(
                    f"Twelve Data request failed; endpoint={endpoint}; "
                    f"error_type={type(error).__name__}"
                ) from None
            time.sleep(min(30.0, 2.0 ** (attempt - 1)))
            continue

        code, message = _safe_json_error(payload)
        rate_limited = response.status_code == 429 or str(code) == "429"
        if rate_limited and attempt < max_attempts:
            header_delay = response.headers.get("Retry-After")
            try:
                delay = float(header_delay)
            except (TypeError, ValueError):
                delay = min(60.0, 5.0 * attempt)
            time.sleep(max(1.0, delay))
            continue

        if response.status_code != 200:
            raise ProviderDepthBacktestError(
                f"Twelve Data request failed; endpoint={endpoint}; "
                f"http_status={response.status_code}; api_code={code}"
            )
        if not isinstance(payload, dict):
            raise ProviderDepthBacktestError(
                f"Twelve Data returned non-object JSON; endpoint={endpoint}"
            )
        if str(payload.get("status") or "").lower() == "error":
            raise ProviderDepthBacktestError(
                f"Twelve Data API error; endpoint={endpoint}; "
                f"api_code={code}; message={str(message or '')[:160]}"
            )
        return payload

    raise ProviderDepthBacktestError(
        f"Twelve Data retries exhausted; endpoint={endpoint}; "
        f"http_status={last_status}"
    )


def fetch_twelvedata_earliest(
    symbol: str,
    *,
    api_key: str,
) -> datetime:
    provider_symbol = TWELVEDATA_SYMBOLS[symbol]
    payload = _request_json(
        "earliest_timestamp",
        params={
            "symbol": provider_symbol,
            "interval": "5min",
            "apikey": api_key,
        },
    )
    value = (
        payload.get("datetime")
        or payload.get("earliest_timestamp")
        or payload.get("timestamp")
    )
    if value in (None, ""):
        raise ProviderDepthBacktestError(
            f"Twelve Data earliest timestamp is unavailable for symbol={symbol}"
        )
    return _utc(value)


def _normalize_provider_frame(
    frame: pd.DataFrame,
    *,
    symbol: str,
) -> pd.DataFrame:
    data = frame.copy()
    if isinstance(data.columns, pd.MultiIndex):
        normalized_columns: list[str] = []
        for column in data.columns:
            parts = [str(part) for part in column if str(part or "").strip()]
            normalized_columns.append(
                next(
                    (
                        part
                        for part in parts
                        if part.lower()
                        in {"open", "high", "low", "close", "volume"}
                    ),
                    parts[0] if parts else "",
                )
            )
        data.columns = normalized_columns
    return normalize_m5_history(data, symbol=symbol)


def _frame_sha256(frame: pd.DataFrame) -> str:
    if frame.empty:
        return hashlib.sha256(b"").hexdigest()
    stable = frame[
        ["bar_open_utc", "open", "high", "low", "close", "volume"]
    ].copy()
    stable["bar_open_utc"] = pd.to_datetime(
        stable["bar_open_utc"],
        utc=True,
    ).map(lambda value: value.isoformat())
    payload = stable.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _assert_no_sensitive_keys(value: Any, path: str = "root") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            lowered = str(key).lower()
            if any(fragment in lowered for fragment in SENSITIVE_KEY_FRAGMENTS):
                raise ProviderDepthBacktestError(
                    f"sensitive key blocked from research artifact: {path}.{key}"
                )
            _assert_no_sensitive_keys(nested, f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _assert_no_sensitive_keys(nested, f"{path}[{index}]")


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def fetch_twelvedata_max_history(
    symbol: str,
    *,
    api_key: str,
    end_utc: datetime,
    pause_seconds: float,
    outputsize: int = DEFAULT_TWELVEDATA_OUTPUTSIZE,
    lookback_days: int = DEFAULT_TWELVEDATA_LOOKBACK_DAYS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    provider_symbol = TWELVEDATA_SYMBOLS[symbol]
    earliest = fetch_twelvedata_earliest(symbol, api_key=api_key)
    requested_start = end_utc - timedelta(days=max(1, int(lookback_days)))
    effective_start = max(earliest, requested_start)
    cursor_end = end_utc
    frames: list[pd.DataFrame] = []
    request_count = 1  # earliest_timestamp
    page_rows: list[int] = []
    previous_oldest: datetime | None = None
    pagination_stop_reason: str | None = None

    for page in range(1, MAX_PROVIDER_PAGES_PER_SYMBOL + 1):
        if cursor_end < effective_start:
            pagination_stop_reason = "CURSOR_REACHED_REQUESTED_BOUNDARY"
            break
        if request_count > 1 and pause_seconds > 0:
            time.sleep(pause_seconds)
        payload = _request_json(
            "time_series",
            params={
                "symbol": provider_symbol,
                "interval": "5min",
                "end_date": cursor_end.strftime("%Y-%m-%d %H:%M:%S"),
                "timezone": "UTC",
                "order": "asc",
                "format": "JSON",
                "outputsize": outputsize,
                "apikey": api_key,
            },
        )
        request_count += 1
        values = payload.get("values")
        if not isinstance(values, list) or not values:
            if frames:
                pagination_stop_reason = "PROVIDER_RETURNED_NO_OLDER_ROWS"
                break
            raise ProviderDepthBacktestError(
                f"Twelve Data returned no M5 rows for symbol={symbol}"
            )

        normalized = _normalize_provider_frame(
            pd.DataFrame(values),
            symbol=symbol,
        )
        normalized = normalized.loc[
            (
                pd.to_datetime(normalized["bar_open_utc"], utc=True)
                >= pd.Timestamp(effective_start)
            )
            & (
                pd.to_datetime(normalized["bar_open_utc"], utc=True)
                <= pd.Timestamp(end_utc)
            )
        ]
        if normalized.empty:
            pagination_stop_reason = "PAGE_EMPTY_AFTER_EARLIEST_FILTER"
            break
        frames.append(normalized)
        page_rows.append(len(normalized))
        oldest = _utc(normalized["bar_open_utc"].iloc[0])

        if previous_oldest is not None and oldest >= previous_oldest:
            raise ProviderDepthBacktestError(
                f"Twelve Data pagination made no backward progress for symbol={symbol}"
            )
        previous_oldest = oldest
        if oldest <= effective_start + timedelta(minutes=5):
            pagination_stop_reason = (
                "PROVIDER_EARLIEST_REACHED"
                if effective_start == earliest
                else "REQUESTED_LOOKBACK_REACHED"
            )
            break
        cursor_end = oldest - timedelta(minutes=5)
    else:
        raise ProviderDepthBacktestError(
            f"Twelve Data pagination safety limit reached for symbol={symbol}"
        )

    history = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    history = normalize_m5_history(history, symbol=symbol)
    if history.empty:
        raise ProviderDepthBacktestError(
            f"Twelve Data normalized history is empty for symbol={symbol}"
        )
    delivered_first = _utc(history["bar_open_utc"].iloc[0])
    delivered_last = _utc(history["bar_open_utc"].iloc[-1])
    return history, {
        "provider": "twelvedata",
        "provider_symbol": provider_symbol,
        "provider_earliest_5m_utc": earliest.isoformat(),
        "requested_start_utc": requested_start.isoformat(),
        "requested_end_utc": end_utc.isoformat(),
        "requested_lookback_days": int(lookback_days),
        "delivered_first_bar_utc": delivered_first.isoformat(),
        "delivered_last_bar_utc": delivered_last.isoformat(),
        "delivered_rows": len(history),
        "provider_requests": request_count,
        "page_count": len(page_rows),
        "page_row_min": min(page_rows) if page_rows else 0,
        "page_row_max": max(page_rows) if page_rows else 0,
        "reached_provider_earliest": (
            delivered_first <= earliest + timedelta(minutes=5)
        ),
        "reached_requested_start": (
            delivered_first <= effective_start + timedelta(minutes=5)
        ),
        "pagination_stop_reason": pagination_stop_reason,
        "history_sha256": _frame_sha256(history),
    }


def fetch_yfinance_max_history(
    symbol: str,
    *,
    end_utc: datetime,
    cache_dir: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    try:
        import yfinance as yf
    except ImportError as error:
        raise ProviderDepthBacktestError(
            "yfinance dependency is unavailable"
        ) from error

    cache_dir.mkdir(parents=True, exist_ok=True)
    yf.set_tz_cache_location(str(cache_dir))
    attempts: list[dict[str, Any]] = []
    selected_ticker: str | None = None
    history = pd.DataFrame()
    requested_start = end_utc - timedelta(days=60)

    for ticker in YFINANCE_SYMBOLS[symbol]:
        attempt: dict[str, Any] = {
            "provider_symbol": ticker,
            "rows": 0,
            "error_type": None,
        }
        try:
            raw = yf.download(
                ticker,
                period="60d",
                interval="5m",
                progress=False,
                auto_adjust=False,
                threads=False,
                timeout=45,
            )
            normalized = _normalize_provider_frame(raw, symbol=symbol)
            if not normalized.empty:
                normalized = normalized.loc[
                    pd.to_datetime(
                        normalized["bar_open_utc"],
                        utc=True,
                    )
                    <= pd.Timestamp(end_utc)
                ]
        except Exception as error:  # noqa: BLE001
            attempt["error_type"] = type(error).__name__
            attempts.append(attempt)
            continue
        attempt["rows"] = len(normalized)
        attempts.append(attempt)
        if not normalized.empty:
            selected_ticker = ticker
            history = normalized
            break

    if history.empty or selected_ticker is None:
        raise ProviderDepthBacktestError(
            f"yfinance returned no M5 rows for symbol={symbol}"
        )
    delivered_first = _utc(history["bar_open_utc"].iloc[0])
    delivered_last = _utc(history["bar_open_utc"].iloc[-1])
    return history, {
        "provider": "yfinance",
        "provider_symbol": selected_ticker,
        "provider_intraday_limit": "60d",
        "requested_start_utc": requested_start.isoformat(),
        "requested_end_utc": end_utc.isoformat(),
        "delivered_first_bar_utc": delivered_first.isoformat(),
        "delivered_last_bar_utc": delivered_last.isoformat(),
        "delivered_rows": len(history),
        "provider_requests": len(attempts),
        "provider_attempts": attempts,
        "history_sha256": _frame_sha256(history),
    }


def fetch_symbol_history(
    symbol: str,
    *,
    end_utc: datetime,
    pause_seconds: float,
    yfinance_cache_dir: Path,
    twelvedata_lookback_days: int = DEFAULT_TWELVEDATA_LOOKBACK_DAYS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if symbol in TWELVEDATA_SYMBOLS:
        api_key = os.getenv("TWELVEDATA_API_KEY")
        if not api_key:
            raise ProviderDepthBacktestError(
                "TWELVEDATA_API_KEY is unavailable"
            )
        return fetch_twelvedata_max_history(
            symbol,
            api_key=api_key,
            end_utc=end_utc,
            pause_seconds=pause_seconds,
            lookback_days=twelvedata_lookback_days,
        )
    if symbol in YFINANCE_SYMBOLS:
        return fetch_yfinance_max_history(
            symbol,
            end_utc=end_utc,
            cache_dir=yfinance_cache_dir,
        )
    raise ProviderDepthBacktestError(
        f"no provider-depth M5 mapping for symbol={symbol}"
    )


def fetch_all_histories(
    *,
    run_dir: Path,
    symbols: Sequence[str],
    pause_seconds: float,
    allow_network_fetch: bool,
    resume: bool,
    twelvedata_lookback_days: int = DEFAULT_TWELVEDATA_LOOKBACK_DAYS,
) -> dict[str, Any]:
    if not allow_network_fetch:
        raise ProviderDepthBacktestError(
            "network fetch requires explicit --allow-network-fetch"
        )
    history_dir = run_dir / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "provider_coverage.json"
    existing: dict[str, Any] = {}
    if resume and manifest_path.exists():
        try:
            loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
            existing = (
                dict(loaded.get("symbols") or {})
                if isinstance(loaded, dict)
                else {}
            )
        except Exception:
            existing = {}

    end_utc = _closed_m5_now()
    coverage_by_symbol: dict[str, Any] = dict(existing)
    for symbol in symbols:
        history_path = history_dir / f"{symbol}_5m.parquet"
        if (
            resume
            and history_path.exists()
            and isinstance(coverage_by_symbol.get(symbol), Mapping)
            and coverage_by_symbol[symbol].get("status") == "OK"
        ):
            continue

        print(
            json.dumps(
                {
                    "event": "provider_fetch_started",
                    "symbol": symbol,
                    "at_utc": datetime.now(UTC).isoformat(),
                },
                sort_keys=True,
            ),
            flush=True,
        )
        started = time.monotonic()
        try:
            history, audit = fetch_symbol_history(
                symbol,
                end_utc=end_utc,
                pause_seconds=pause_seconds,
                yfinance_cache_dir=run_dir / "yfinance-cache",
                twelvedata_lookback_days=twelvedata_lookback_days,
            )
            _atomic_parquet(history_path, history)
            audit = {
                **audit,
                "status": "OK",
                "artifact": str(history_path.relative_to(run_dir)),
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
            coverage_by_symbol[symbol] = audit
            print(
                json.dumps(
                    {
                        "event": "provider_fetch_completed",
                        "symbol": symbol,
                        "rows": audit["delivered_rows"],
                        "first_bar_utc": audit["delivered_first_bar_utc"],
                        "last_bar_utc": audit["delivered_last_bar_utc"],
                        "requests": audit["provider_requests"],
                        "elapsed_seconds": audit["elapsed_seconds"],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        except Exception as error:  # noqa: BLE001
            coverage_by_symbol[symbol] = {
                "status": "FAILED",
                "error_type": type(error).__name__,
                "error": str(error)[:240],
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
            manifest = {
                "version": RUNNER_VERSION,
                "updated_at_utc": datetime.now(UTC).isoformat(),
                "symbols": coverage_by_symbol,
            }
            _assert_no_sensitive_keys(manifest)
            _atomic_json(manifest_path, manifest)
            raise

        manifest = {
            "version": RUNNER_VERSION,
            "updated_at_utc": datetime.now(UTC).isoformat(),
            "symbols": coverage_by_symbol,
        }
        _assert_no_sensitive_keys(manifest)
        _atomic_json(manifest_path, manifest)

    return {
        "version": RUNNER_VERSION,
        "updated_at_utc": datetime.now(UTC).isoformat(),
        "symbols": coverage_by_symbol,
    }


def _has_positioning_field(value: Any, field_names: set[str]) -> bool:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if str(key) in field_names and nested not in (None, ""):
                return True
            if _has_positioning_field(nested, field_names):
                return True
    elif isinstance(value, list):
        return any(
            _has_positioning_field(nested, field_names)
            for nested in value
        )
    return False


def summarize_operational_positioning_history(
    history_path: Path,
) -> dict[str, Any]:
    """Summarize only snapshots that were actually persisted in production."""

    if not history_path.exists():
        return {
            "status": "NO_HISTORICAL_SNAPSHOTS",
            "snapshot_count": 0,
            "earliest_snapshot_utc": None,
            "latest_snapshot_utc": None,
            "symbols": {},
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }

    oi_fields = {
        "open_interest",
        "open_interest_change_pct",
        "perp_open_interest_change_pct",
        "cme_open_interest_change_pct",
    }
    funding_fields = {"funding_rate", "funding_rate_pct"}
    timestamps: list[str] = []
    parsed_snapshots = 0
    snapshots_with_oi = 0
    snapshots_with_funding = 0
    symbol_counts: dict[str, dict[str, int]] = {}
    invalid_lines = 0

    with history_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                snapshot = json.loads(text)
            except json.JSONDecodeError:
                invalid_lines += 1
                continue
            if not isinstance(snapshot, Mapping):
                invalid_lines += 1
                continue

            parsed_snapshots += 1
            timestamp = str(
                snapshot.get("generated_at")
                or snapshot.get("date")
                or ""
            ).strip()
            if timestamp:
                timestamps.append(timestamp)

            snapshot_has_oi = False
            snapshot_has_funding = False
            items = snapshot.get("items")
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                symbol = str(item.get("symbol") or "").strip().upper()
                if not symbol:
                    continue
                counts = symbol_counts.setdefault(
                    symbol,
                    {
                        "snapshot_count": 0,
                        "oi_snapshot_count": 0,
                        "funding_snapshot_count": 0,
                    },
                )
                counts["snapshot_count"] += 1
                has_oi = _has_positioning_field(item, oi_fields)
                has_funding = _has_positioning_field(item, funding_fields)
                if has_oi:
                    counts["oi_snapshot_count"] += 1
                    snapshot_has_oi = True
                if has_funding:
                    counts["funding_snapshot_count"] += 1
                    snapshot_has_funding = True

            if snapshot_has_oi:
                snapshots_with_oi += 1
            if snapshot_has_funding:
                snapshots_with_funding += 1

    if parsed_snapshots == 0:
        status = "NO_HISTORICAL_SNAPSHOTS"
    elif snapshots_with_oi or snapshots_with_funding:
        status = "AVAILABLE"
    else:
        status = "SNAPSHOTS_WITHOUT_OI_FUNDING"

    timestamps.sort()
    return {
        "status": status,
        "snapshot_count": parsed_snapshots,
        "snapshots_with_open_interest": snapshots_with_oi,
        "snapshots_with_funding": snapshots_with_funding,
        "earliest_snapshot_utc": timestamps[0] if timestamps else None,
        "latest_snapshot_utc": timestamps[-1] if timestamps else None,
        "invalid_lines": invalid_lines,
        "symbols": dict(sorted(symbol_counts.items())),
        "usage": (
            "coverage_only_where_persisted; no historical values were "
            "reconstructed"
        ),
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }


def collect_positioning_coverage(
    *,
    target_date: str,
    cot_lookback_weeks: int,
    operational_history_path: Path,
) -> dict[str, Any]:
    """Collect slow COT coverage and persisted operational snapshot coverage."""

    try:
        from app.services.positioning.collectors.cftc_cot_collector import (
            collect_cftc_cot_snapshot,
        )

        cot = collect_cftc_cot_snapshot(
            target_date=target_date,
            lookback_weeks=cot_lookback_weeks,
        )
        cot_items = []
        for item in cot.get("items") or []:
            if not isinstance(item, Mapping):
                continue
            normalization = (
                item.get("normalization")
                if isinstance(item.get("normalization"), Mapping)
                else {}
            )
            interpretation = (
                item.get("interpretation")
                if isinstance(item.get("interpretation"), Mapping)
                else {}
            )
            quality = (
                item.get("data_quality")
                if isinstance(item.get("data_quality"), Mapping)
                else {}
            )
            cot_items.append(
                {
                    "symbol": item.get("symbol"),
                    "report_date": item.get("report_date"),
                    "history_weeks": item.get("history_weeks"),
                    "lookback_weeks_requested": normalization.get(
                        "lookback_weeks_requested"
                    ),
                    "primary_tag": interpretation.get("primary_tag"),
                    "data_quality": quality.get("status"),
                }
            )
        weekly_cot = {
            "status": cot.get("status"),
            "requested_lookback_weeks": cot_lookback_weeks,
            "report_date_latest": cot.get("report_date_latest"),
            "symbol_count": len(cot_items),
            "symbols": cot_items,
            "usage": (
                "slow_context_coverage_only; excluded from execution winrate "
                "because COT has no Battle Gate or Telegram signal impact"
            ),
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }
    except Exception as error:  # noqa: BLE001
        weekly_cot = {
            "status": "ERROR",
            "requested_lookback_weeks": cot_lookback_weeks,
            "report_date_latest": None,
            "symbol_count": 0,
            "symbols": [],
            "error_type": type(error).__name__,
            "error": str(error)[:240],
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }

    return {
        "weekly_cot": weekly_cot,
        "operational_oi_funding": summarize_operational_positioning_history(
            operational_history_path
        ),
        "winrate_policy": (
            "positioning is reported as research-only coverage and does not "
            "alter the core LTF execution winrate"
        ),
    }


def load_execution_cost_models(
    path: Path | None,
) -> dict[str, ExecutionCostModel]:
    """Load explicit per-symbol research costs without inventing defaults."""

    if path is None:
        return {}
    if not path.exists() or not path.is_file():
        raise ProviderDepthBacktestError(
            f"execution cost model file is missing: {path}"
        )
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except Exception as error:  # noqa: BLE001
        raise ProviderDepthBacktestError(
            "execution cost model JSON is invalid; "
            f"error_type={type(error).__name__}"
        ) from None
    if not isinstance(document, Mapping):
        raise ProviderDepthBacktestError(
            "execution cost model JSON must be an object"
        )
    raw_symbols = (
        document.get("symbols")
        if isinstance(document.get("symbols"), Mapping)
        else document
    )
    models: dict[str, ExecutionCostModel] = {}
    for raw_symbol, raw_model in raw_symbols.items():
        symbol = str(raw_symbol).upper()
        if symbol not in TWELVEDATA_SYMBOLS and symbol not in YFINANCE_SYMBOLS:
            raise ProviderDepthBacktestError(
                f"execution cost model contains unsupported symbol={symbol}"
            )
        if not isinstance(raw_model, Mapping):
            raise ProviderDepthBacktestError(
                f"execution cost model must be an object for symbol={symbol}"
            )
        try:
            models[symbol] = ExecutionCostModel.from_mapping(raw_model)
        except (TypeError, ValueError) as error:
            raise ProviderDepthBacktestError(
                f"execution cost model is invalid for symbol={symbol}: {error}"
            ) from None
    return models


def run_streamed_backtest(
    *,
    run_dir: Path,
    symbols: Sequence[str],
    provider_manifest: Mapping[str, Any],
    holdout_fraction: float,
    primary_development_r: float = DEFAULT_PRIMARY_DEVELOPMENT_R,
    cost_models: Mapping[str, ExecutionCostModel] | None = None,
) -> dict[str, Any]:
    candidates: list[HistoricalWatchCandidate] = []
    records: list[dict[str, Any]] = []
    event_records: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    provider_symbols = provider_manifest.get("symbols")
    if not isinstance(provider_symbols, Mapping):
        raise ProviderDepthBacktestError("provider coverage manifest is invalid")

    for symbol in symbols:
        provider_audit = provider_symbols.get(symbol)
        if not isinstance(provider_audit, Mapping) or provider_audit.get("status") != "OK":
            raise ProviderDepthBacktestError(
                f"provider coverage is incomplete for symbol={symbol}"
            )
        history_path = run_dir / str(
            provider_audit.get("artifact") or f"history/{symbol}_5m.parquet"
        )
        if not history_path.exists():
            raise ProviderDepthBacktestError(
                f"history artifact is missing for symbol={symbol}"
            )

        print(
            json.dumps(
                {
                    "event": "symbol_replay_started",
                    "symbol": symbol,
                    "at_utc": datetime.now(UTC).isoformat(),
                },
                sort_keys=True,
            ),
            flush=True,
        )
        started = time.monotonic()
        history = normalize_m5_history(
            pd.read_parquet(history_path),
            symbol=symbol,
        )
        event_candidates, symbol_coverage = reconstruct_tpo_watch_candidates(
            history,
            symbol=symbol,
            include_counter_htf_events=True,
        )
        symbol_candidates = [
            candidate
            for candidate in event_candidates
            if candidate.payload.get("signal_alignment") != "COUNTER_TREND"
        ]
        symbol_coverage["event_candidate_count"] = len(event_candidates)
        symbol_coverage["execution_candidate_count"] = len(
            symbol_candidates
        )
        symbol_coverage["candidate_count"] = len(symbol_candidates)
        symbol_rows: list[dict[str, Any]] = []
        symbol_event_rows = [
            measure_event_development(
                candidate,
                history,
                primary_development_r=primary_development_r,
            )
            for candidate in event_candidates
        ]
        for index, candidate in enumerate(symbol_candidates, start=1):
            symbol_rows.append(
                replay_candidate(
                    candidate,
                    history,
                    cost_model=(cost_models or {}).get(symbol),
                )
            )
            if index % 100 == 0:
                print(
                    json.dumps(
                        {
                            "event": "symbol_replay_progress",
                            "symbol": symbol,
                            "completed_candidates": index,
                            "candidate_count": len(symbol_candidates),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )

        if not symbol_candidates and event_candidates:
            print(
                json.dumps(
                    {
                        "event": "symbol_execution_universe_empty",
                        "symbol": symbol,
                        "event_candidate_count": len(event_candidates),
                        "reason": "ALL_EVENTS_EXCLUDED_BY_HARD_GATE",
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

        symbol_coverage["provider"] = provider_audit.get("provider")
        symbol_coverage["provider_earliest_5m_utc"] = provider_audit.get(
            "provider_earliest_5m_utc"
        )
        symbol_coverage["provider_intraday_limit"] = provider_audit.get(
            "provider_intraday_limit"
        )
        symbol_coverage["provider_reached_earliest"] = provider_audit.get(
            "reached_provider_earliest"
        )
        symbol_coverage["replay_elapsed_seconds"] = round(
            time.monotonic() - started,
            3,
        )
        candidates.extend(symbol_candidates)
        records.extend(symbol_rows)
        event_records.extend(symbol_event_rows)
        coverage.append(symbol_coverage)
        del history

        print(
            json.dumps(
                {
                    "event": "symbol_replay_completed",
                    "symbol": symbol,
                    "event_candidate_count": len(event_candidates),
                    "execution_candidate_count": len(symbol_candidates),
                    "ready_count": sum(
                        1 for row in symbol_rows if bool(row.get("ready"))
                    ),
                    "developed_count": sum(
                        1
                        for row in symbol_event_rows
                        if bool(row.get("primary_development_reached"))
                    ),
                    "elapsed_seconds": symbol_coverage[
                        "replay_elapsed_seconds"
                    ],
                },
                sort_keys=True,
            ),
            flush=True,
        )

    report = compile_backtest_report(
        candidates=candidates,
        rows=records,
        event_records=event_records,
        coverage=coverage,
        holdout_fraction=holdout_fraction,
        primary_development_r=primary_development_r,
    )
    report["provider_depth"] = dict(provider_manifest)
    report["run_dir"] = str(run_dir)
    effective_models = {
        symbol: (
            (cost_models or {}).get(symbol) or ExecutionCostModel()
        ).to_dict()
        for symbol in symbols
    }
    report["execution_cost_models"] = dict(sorted(effective_models.items()))
    report["execution_integrity"]["cost_model_status"] = (
        "EXPLICIT_PER_SYMBOL_CONFIG"
        if cost_models
        else "UNCONFIGURED_ZERO_COST"
    )
    return report


def _pct(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value) * 100.0:.2f}%"


def _number(value: Any, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    number = float(value)
    if math.isinf(number):
        return "∞"
    return f"{number:.{digits}f}"


def render_markdown_report(report: Mapping[str, Any]) -> str:
    metrics = report["metrics"]
    all_metrics = metrics["all"]
    holdout = metrics["holdout"]
    event_census = (
        report.get("event_census")
        if isinstance(report.get("event_census"), Mapping)
        else {}
    )
    census_metrics = (
        event_census.get("metrics")
        if isinstance(event_census.get("metrics"), Mapping)
        else {}
    )
    census_all = (
        census_metrics.get("all")
        if isinstance(census_metrics.get("all"), Mapping)
        else {}
    )
    census_holdout = (
        census_metrics.get("holdout")
        if isinstance(census_metrics.get("holdout"), Mapping)
        else {}
    )
    primary_development_r = (
        event_census.get("definition", {}).get("primary_development_R")
        if isinstance(event_census.get("definition"), Mapping)
        else None
    )
    coverage_rows = []
    for item in report.get("coverage") or []:
        coverage_rows.append(
            "| {symbol} | {first} | {last} | {events} | {candidates} | {provider} |".format(
                symbol=item.get("symbol"),
                first=item.get("history_first_bar_utc"),
                last=item.get("history_last_bar_utc"),
                events=item.get(
                    "event_candidate_count",
                    item.get("candidate_count"),
                ),
                candidates=item.get(
                    "execution_candidate_count",
                    item.get("candidate_count"),
                ),
                provider=item.get("provider"),
            )
        )

    by_symbol_rows = []
    for symbol, values in sorted((metrics.get("by_symbol") or {}).items()):
        by_symbol_rows.append(
            "| {symbol} | {candidates} | {ready} | {filled} | {winrate} | {gross_r} | {net_r} |".format(
                symbol=symbol,
                candidates=values.get("candidate_count"),
                ready=values.get("ready_count"),
                filled=values.get("filled_count"),
                winrate=_pct(values.get("winrate_closed")),
                gross_r=_number(values.get("average_gross_R_filled")),
                net_r=_number(values.get("average_net_R_filled")),
            )
        )

    census_cohort_rows = []
    for key, values in sorted(
        (
            census_metrics.get("by_symbol_family_direction") or {}
        ).items()
    ):
        symbol, family, direction = (
            [*str(key).split("|", 2), "UNKNOWN", "UNKNOWN"][:3]
        )
        execution = (
            values.get("execution")
            if isinstance(values.get("execution"), Mapping)
            else {}
        )
        census_cohort_rows.append(
            "| {symbol} | {family} | {direction} | {events} | {developed} | "
            "{development_rate} | {filled} | {winrate} | {net_r} |".format(
                symbol=symbol,
                family=family,
                direction=direction,
                events=values.get("event_count"),
                developed=values.get("developed_count"),
                development_rate=_pct(values.get("development_rate")),
                filled=execution.get("filled_count"),
                winrate=_pct(execution.get("trade_winrate_closed")),
                net_r=_number(execution.get("average_net_R_filled")),
            )
        )

    positioning = (
        report.get("positioning_coverage")
        if isinstance(report.get("positioning_coverage"), Mapping)
        else {}
    )
    weekly_cot = (
        positioning.get("weekly_cot")
        if isinstance(positioning.get("weekly_cot"), Mapping)
        else {}
    )
    operational = (
        positioning.get("operational_oi_funding")
        if isinstance(positioning.get("operational_oi_funding"), Mapping)
        else {}
    )
    cot_rows = []
    for item in weekly_cot.get("symbols") or []:
        if not isinstance(item, Mapping):
            continue
        cot_rows.append(
            "| {symbol} | {weeks} | {report_date} | {tag} | {quality} |".format(
                symbol=item.get("symbol"),
                weeks=item.get("history_weeks"),
                report_date=item.get("report_date"),
                tag=item.get("primary_tag"),
                quality=item.get("data_quality"),
            )
        )

    limitations = report.get("research_scope") or {}
    integrity = report.get("execution_integrity") or {}
    return "\n".join(
        [
            "# Backtest Integrity v2.0 — OTD/ORR Event Census",
            "",
            f"Generated: `{report.get('generated_at_utc')}`  ",
            f"Engine: `{report.get('engine_version')}`  ",
            f"Mode: `{report.get('mode')}`",
            "",
            "## Primary results",
            "",
            "| Metric | Full history | Chronological 30% holdout |",
            "|---|---:|---:|",
            f"| Candidates | {all_metrics.get('candidate_count')} | {holdout.get('candidate_count')} |",
            f"| ENTRY_READY | {all_metrics.get('ready_count')} | {holdout.get('ready_count')} |",
            f"| Filled | {all_metrics.get('filled_count')} | {holdout.get('filled_count')} |",
            f"| Closed-trade win rate | {_pct(all_metrics.get('winrate_closed'))} | {_pct(holdout.get('winrate_closed'))} |",
            f"| Average gross R, filled | {_number(all_metrics.get('average_gross_R_filled'))} | {_number(holdout.get('average_gross_R_filled'))} |",
            f"| Average net R, filled | {_number(all_metrics.get('average_net_R_filled'))} | {_number(holdout.get('average_net_R_filled'))} |",
            f"| Gross total R | {_number(all_metrics.get('gross_total_R'))} | {_number(holdout.get('gross_total_R'))} |",
            f"| Net total R | {_number(all_metrics.get('net_total_R'))} | {_number(holdout.get('net_total_R'))} |",
            f"| Gross profit factor | {_number(all_metrics.get('gross_profit_factor'))} | {_number(holdout.get('gross_profit_factor'))} |",
            f"| Net profit factor | {_number(all_metrics.get('net_profit_factor'))} | {_number(holdout.get('net_profit_factor'))} |",
            f"| Entry-window expired, unfilled | {all_metrics.get('entry_window_expired_unfilled_count')} | {holdout.get('entry_window_expired_unfilled_count')} |",
            f"| Context cancelled before fill | {all_metrics.get('context_cancelled_before_fill_count')} | {holdout.get('context_cancelled_before_fill_count')} |",
            f"| Invalidated before fill | {all_metrics.get('invalidated_before_fill_count')} | {holdout.get('invalidated_before_fill_count')} |",
            f"| Filled signals/week | {_number(all_metrics.get('filled_signals_per_week'))} | {_number(holdout.get('filled_signals_per_week'))} |",
            "",
            "## OTD/ORR event development",
            "",
            f"Primary development threshold: `{_number(primary_development_r, 2)}R`. "
            "Development rate and trade win rate use separate denominators.",
            "",
            "| Metric | Full history | Chronological holdout |",
            "|---|---:|---:|",
            f"| Events | {census_all.get('event_count')} | {census_holdout.get('event_count')} |",
            f"| Causally evaluable | {census_all.get('event_evaluable_count')} | {census_holdout.get('event_evaluable_count')} |",
            f"| Developed | {census_all.get('developed_count')} | {census_holdout.get('developed_count')} |",
            f"| Development rate | {_pct(census_all.get('development_rate'))} | {_pct(census_holdout.get('development_rate'))} |",
            f"| Median event MFE | {_number(census_all.get('median_event_MFE_R'))}R | {_number(census_holdout.get('median_event_MFE_R'))}R |",
            f"| Median event MAE | {_number(census_all.get('median_event_MAE_R'))}R | {_number(census_holdout.get('median_event_MAE_R'))}R |",
            f"| Filled entry model | {census_all.get('execution', {}).get('filled_count')} | {census_holdout.get('execution', {}).get('filled_count')} |",
            f"| Closed-trade win rate | {_pct(census_all.get('execution', {}).get('trade_winrate_closed'))} | {_pct(census_holdout.get('execution', {}).get('trade_winrate_closed'))} |",
            f"| Average net R, filled | {_number(census_all.get('execution', {}).get('average_net_R_filled'))} | {_number(census_holdout.get('execution', {}).get('average_net_R_filled'))} |",
            "",
            "## Event census by asset, family and direction",
            "",
            "| Symbol | Family | Direction | Events | Developed | Development rate | Filled | Trade win rate | Avg net R |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|",
            *census_cohort_rows,
            "",
            "## Integrity controls",
            "",
            f"- Entry and trade deadlines are separate: "
            f"`{integrity.get('deadlines_are_separate')}`.",
            f"- Dynamic context cancellation: "
            f"`{integrity.get('dynamic_context_timeline')}`.",
            f"- Cost model status: `{integrity.get('cost_model_status')}`; "
            "primary expectancy uses net R.",
            "- Unconfigured costs remain explicit zero; no broker costs are "
            "silently guessed.",
            "- Event R uses only confirmation close and the structural test "
            "extreme already known at confirmation.",
            "- Same-bar development plus invalidation/context cancellation is "
            "ambiguous and excluded from the development denominator.",
            "- Counter-HTF OTD/ORR remain in the event census to avoid "
            "survivorship bias, but the execution hard gate still excludes them.",
            "",
            "## Provider coverage",
            "",
            "| Symbol | First M5 bar | Last M5 bar | Events | Execution candidates | Provider |",
            "|---|---|---|---:|---:|---|",
            *coverage_rows,
            "",
            "## By symbol",
            "",
            "| Symbol | Candidates | Ready | Filled | Win rate | Avg gross R | Avg net R |",
            "|---|---:|---:|---:|---:|---:|---:|",
            *by_symbol_rows,
            "",
            "## Positioning coverage",
            "",
            f"- Weekly COT status: `{weekly_cot.get('status')}`; requested "
            f"lookback: `{weekly_cot.get('requested_lookback_weeks')}` weeks; "
            f"latest report: `{weekly_cot.get('report_date_latest')}`.",
            f"- Operational OI/funding status: `{operational.get('status')}`; "
            f"persisted snapshots: `{operational.get('snapshot_count')}`; "
            f"with OI: `{operational.get('snapshots_with_open_interest')}`; "
            f"with funding: `{operational.get('snapshots_with_funding')}`.",
            "- Positioning remains research-only and is excluded from the core "
            "LTF execution winrate.",
            "- Per-event weekly COT cohorts remain `NO_DATA` until actual "
            "publication timestamps can be joined causally; report dates alone "
            "are not treated as availability timestamps.",
            "- Per-event operational positioning remains `NO_DATA` outside the "
            "coverage of persisted as-of snapshots.",
            "",
            "| Symbol | COT weeks | Latest report | Current tag | Quality |",
            "|---|---:|---|---|---|",
            *cot_rows,
            "",
            "## Interpretation limits",
            "",
            "- This is a no-look-ahead execution-layer replay on reconstructed "
            "TPO Open Test Drive and Open Rejection Reverse candidates.",
            "- Historical macro, positioning and Battle Gate states were not "
            "invented; the result is conditional LTF performance, not yet the "
            "fully filtered production-system win rate.",
            "- Same-bar entry+target events are excluded as ambiguous; same-bar "
            "stop+target events are scored stop-first.",
            "- The census does not infer the best invalidation from one fixed "
            "basis; alternative invalidation models require a later controlled "
            "comparison.",
            "- A limit can fill only through its production entry window; a "
            "filled trade can continue through the separate resolution horizon.",
            "- Session/Profile parity is deliberately deferred to P1 and is not "
            "claimed by this corrected execution-only result.",
            f"- Battle Gate impact: `{limitations.get('battle_gate_impact')}`; "
            f"Telegram impact: `{limitations.get('telegram_signal_impact')}`.",
            "",
        ]
    )


def write_report_artifacts(
    *,
    run_dir: Path,
    report: Mapping[str, Any],
) -> dict[str, Any]:
    report_path = run_dir / "backtest_report.json"
    markdown_path = run_dir / "backtest_report.md"
    records_path = run_dir / "backtest_records.parquet"
    census_path = run_dir / "otd_orr_event_census.parquet"
    census_csv_path = run_dir / "otd_orr_event_census.csv"
    _assert_no_sensitive_keys(report)
    _atomic_json(report_path, report)
    markdown_temporary = markdown_path.with_suffix(
        markdown_path.suffix + ".tmp"
    )
    markdown_temporary.write_text(
        render_markdown_report(report),
        encoding="utf-8",
    )
    markdown_temporary.replace(markdown_path)
    record_frame = pd.DataFrame(report.get("records") or [])
    if not record_frame.empty:
        for column in (
            "blockers",
            "transition_history",
            "context_transition_history",
            "execution_cost_model",
        ):
            if column in record_frame.columns:
                record_frame[column] = record_frame[column].map(
                    lambda value: json.dumps(
                        value,
                        sort_keys=True,
                        ensure_ascii=False,
                    )
                )
        _atomic_parquet(records_path, record_frame)
    event_census = report.get("event_census")
    census_records = (
        event_census.get("records")
        if isinstance(event_census, Mapping)
        else []
    )
    census_frame = pd.DataFrame(census_records or [])
    if not census_frame.empty:
        for column in (
            "threshold_hits_utc",
            "ambiguous_thresholds_R",
        ):
            if column in census_frame.columns:
                census_frame[column] = census_frame[column].map(
                    lambda value: json.dumps(
                        value,
                        sort_keys=True,
                        ensure_ascii=False,
                    )
                )
        _atomic_parquet(census_path, census_frame)
        census_csv_temporary = census_csv_path.with_suffix(
            census_csv_path.suffix + ".tmp"
        )
        census_frame.to_csv(census_csv_temporary, index=False)
        census_csv_temporary.replace(census_csv_path)
    return {
        "report_json": str(report_path),
        "report_markdown": str(markdown_path),
        "records_parquet": (
            str(records_path) if records_path.exists() else None
        ),
        "event_census_parquet": (
            str(census_path) if census_path.exists() else None
        ),
        "event_census_csv": (
            str(census_csv_path) if census_csv_path.exists() else None
        ),
    }


def _run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch maximum provider-delivered M5 history and backtest "
            "LTF Execution State Machine v2."
        )
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--symbols",
        default=",".join(_active_symbols()),
        help="Comma-separated canonical symbols.",
    )
    parser.add_argument(
        "--provider-pause-seconds",
        default=DEFAULT_PROVIDER_PAUSE_SECONDS,
        type=float,
    )
    parser.add_argument(
        "--twelvedata-lookback-days",
        default=DEFAULT_TWELVEDATA_LOOKBACK_DAYS,
        type=int,
        help="Twelve Data M5 research window; approved range is 365-730 days.",
    )
    parser.add_argument(
        "--cot-lookback-weeks",
        default=DEFAULT_CFTC_LOOKBACK_WEEKS,
        type=int,
    )
    parser.add_argument(
        "--operational-history-path",
        default=DEFAULT_OPERATIONAL_HISTORY_PATH,
        type=Path,
    )
    parser.add_argument(
        "--cost-model-json",
        default=None,
        type=Path,
        help=(
            "Optional per-symbol spread/slippage/commission research config. "
            "When omitted, costs are explicitly reported as unconfigured zero."
        ),
    )
    parser.add_argument("--holdout-fraction", default=0.30, type=float)
    parser.add_argument(
        "--development-threshold-r",
        default=DEFAULT_PRIMARY_DEVELOPMENT_R,
        type=float,
        help=(
            "Primary event-development threshold in structural R. "
            "Default: 1.5R."
        ),
    )
    parser.add_argument("--allow-network-fetch", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument(
        "--replay-only",
        action="store_true",
        help="Use already fetched history in the selected run directory.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.provider_pause_seconds < 0:
        raise ProviderDepthBacktestError(
            "provider pause seconds cannot be negative"
        )
    if not 365 <= args.twelvedata_lookback_days <= 730:
        raise ProviderDepthBacktestError(
            "Twelve Data lookback must be between 365 and 730 days"
        )
    if args.cot_lookback_weeks != DEFAULT_CFTC_LOOKBACK_WEEKS:
        raise ProviderDepthBacktestError(
            f"COT lookback must be {DEFAULT_CFTC_LOOKBACK_WEEKS} weeks"
        )
    if (
        not math.isfinite(args.development_threshold_r)
        or args.development_threshold_r <= 0
    ):
        raise ProviderDepthBacktestError(
            "development threshold R must be a finite positive number"
        )
    symbols = [
        item.strip().upper()
        for item in str(args.symbols).split(",")
        if item.strip()
    ]
    if not symbols:
        raise ProviderDepthBacktestError("at least one symbol is required")
    unsupported = sorted(
        set(symbols).difference(TWELVEDATA_SYMBOLS).difference(YFINANCE_SYMBOLS)
    )
    if unsupported:
        raise ProviderDepthBacktestError(
            f"unsupported symbols: {', '.join(unsupported)}"
        )
    cost_models = load_execution_cost_models(args.cost_model_json)

    run_id = str(args.run_id or _run_id())
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "provider_coverage.json"
    if args.replay_only:
        if not manifest_path.exists():
            raise ProviderDepthBacktestError(
                f"provider coverage manifest is missing: {manifest_path}"
            )
        provider_manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )
    else:
        provider_manifest = fetch_all_histories(
            run_dir=run_dir,
            symbols=symbols,
            pause_seconds=args.provider_pause_seconds,
            allow_network_fetch=args.allow_network_fetch,
            resume=args.resume,
            twelvedata_lookback_days=args.twelvedata_lookback_days,
        )

    report = run_streamed_backtest(
        run_dir=run_dir,
        symbols=symbols,
        provider_manifest=provider_manifest,
        holdout_fraction=args.holdout_fraction,
        primary_development_r=args.development_threshold_r,
        cost_models=cost_models,
    )
    report["positioning_coverage"] = collect_positioning_coverage(
        target_date=_closed_m5_now().date().isoformat(),
        cot_lookback_weeks=args.cot_lookback_weeks,
        operational_history_path=args.operational_history_path,
    )
    artifacts = write_report_artifacts(run_dir=run_dir, report=report)
    result = {
        "status": "OK",
        "version": RUNNER_VERSION,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "symbols": symbols,
        "artifacts": artifacts,
        "metrics": report["metrics"],
        "event_census_metrics": report.get("event_census", {}).get(
            "metrics"
        ),
    }
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
