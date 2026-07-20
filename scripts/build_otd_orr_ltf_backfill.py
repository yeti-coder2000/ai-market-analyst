from __future__ import annotations

"""Build an isolated OTD/ORR lower-timeframe research dataset.

This module is deliberately outside production runners and services.  It reads a
versioned Statistics integrity artifact plus the existing 15-minute Parquet
cache, obtains 5-minute OHLC for the same signal lifecycles, and writes a new
versioned research artifact.  It never mutates production cache/runtime files or
changes signal permissions.

The key replay invariant is explicit bar-close availability.  Provider
timestamps are treated conservatively as bar-open timestamps, so a 5-minute bar
at ``T`` is unavailable until ``T + 5m`` and a 15-minute bar is unavailable until
``T + 15m``.
"""

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any, Iterable, Iterator, Mapping, Sequence

import pandas as pd


BACKFILL_VERSION = "otd-orr-ltf-backfill-v0.1.1-research-only"
ARTIFACT_SCHEMA_VERSION = "otd-orr-ltf-dataset-v0.1.1"
ARTIFACT_MODE = "RESEARCH_ONLY_VERSIONED_NON_OVERWRITING"

RESEARCH_SCOPE = "RESEARCH_COUNTERFACTUAL"
COHORT_FAMILIES = frozenset(
    {
        "TPO_OPEN_TEST_DRIVE",
        "TPO_OPEN_REJECTION_REVERSE",
    }
)

DEFAULT_CUTOFF_UTC = "2026-06-18T00:00:00+00:00"
DEFAULT_EXPECTED_COHORT_SIZE = 25
DEFAULT_PRE_EVENT_BARS = 120
DEFAULT_FETCH_LOOKBACK_DAYS = 7
DEFAULT_PROVIDER_CHUNK_DAYS = 14
DEFAULT_TWELVEDATA_OUTPUTSIZE = 5000

BAR_COLUMNS = [
    "symbol",
    "timeframe",
    "bar_open_utc",
    "bar_close_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source_provider",
]

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

YFINANCE_SYMBOLS: dict[str, str] = {
    "GER40": "^GDAXI",
    "NAS100": "^NDX",
    "SPX500": "^GSPC",
    "UKOIL": "BZ=F",
}

SENSITIVE_KEY_FRAGMENTS = (
    "api_key",
    "apikey",
    "authorization",
    "password",
    "secret",
    "token",
)


class BackfillError(RuntimeError):
    """Base error for fail-closed research dataset construction."""


class CohortIntegrityError(BackfillError):
    """Raised when the Statistics cohort is ambiguous or incomplete."""


class CoverageError(BackfillError):
    """Raised when timestamped OHLC cannot cover every signal lifecycle."""


class ProviderFetchError(BackfillError):
    """Raised with sanitized provider context and no credential material."""


@dataclass(frozen=True, slots=True)
class CohortRecord:
    signal_id: str
    symbol: str
    scenario_family: str
    scenario_type: str | None
    direction: str | None
    signal_created_at_utc: datetime
    decision_time_utc: datetime
    selected_evaluation_at_utc: datetime
    selected_evaluation_within_original_lifecycle: bool
    selected_evaluation_lag_minutes: float
    selected_evaluation_after_expiry_minutes: float | None
    expires_at_utc: datetime
    htf_bias: str | None
    signal_alignment: str | None
    macro_guard_status: str | None
    stop_quality: str | None
    battle_permission: str | None
    battle_permission_blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "symbol": self.symbol,
            "scenario_family": self.scenario_family,
            "scenario_type": self.scenario_type,
            "direction": self.direction,
            "signal_created_at_utc": _iso_utc(self.signal_created_at_utc),
            "decision_time_utc": _iso_utc(self.decision_time_utc),
            "decision_time_source": "signal_created_at_utc",
            "selected_evaluation_audit": {
                "selected_evaluation_at_utc": _iso_utc(
                    self.selected_evaluation_at_utc
                ),
                "within_original_lifecycle": (
                    self.selected_evaluation_within_original_lifecycle
                ),
                "lag_from_signal_creation_minutes": (
                    self.selected_evaluation_lag_minutes
                ),
                "after_expiry_minutes": (
                    self.selected_evaluation_after_expiry_minutes
                ),
                "context_use": (
                    "AUDIT_ONLY_UNTIL_SELECTED_EVALUATION_TIMESTAMP"
                    if self.selected_evaluation_within_original_lifecycle
                    else "AUDIT_ONLY_OUTSIDE_ORIGINAL_LIFECYCLE"
                ),
            },
            "expires_at_utc": _iso_utc(self.expires_at_utc),
            "htf_bias": self.htf_bias,
            "signal_alignment": self.signal_alignment,
            "macro_guard_status": self.macro_guard_status,
            "stop_quality": self.stop_quality,
            "battle_permission": self.battle_permission,
            "battle_permission_blockers": list(self.battle_permission_blockers),
        }


@dataclass(frozen=True, slots=True)
class FetchWindow:
    symbol: str
    start_utc: datetime
    end_utc: datetime

    def to_dict(self) -> dict[str, str]:
        return {
            "symbol": self.symbol,
            "start_utc": _iso_utc(self.start_utc),
            "end_utc": _iso_utc(self.end_utc),
        }


@dataclass(frozen=True, slots=True)
class NormalizedBars:
    frame: pd.DataFrame
    duplicate_rows_discarded: int
    invalid_rows_discarded: int


def _iso_utc(value: datetime | pd.Timestamp) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat()


def _parse_utc(value: Any, *, field_name: str) -> datetime:
    if value in (None, ""):
        raise CohortIntegrityError(f"Missing required UTC field: {field_name}")
    try:
        timestamp = pd.Timestamp(value)
    except Exception as error:  # noqa: BLE001
        raise CohortIntegrityError(f"Invalid UTC field: {field_name}") from error
    if pd.isna(timestamp):
        raise CohortIntegrityError(f"Invalid UTC field: {field_name}")
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.to_pydatetime()


def _walk_objects(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_objects(child)


def _safe_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _safe_text_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    return tuple(str(item) for item in value if str(item or "").strip())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_frame(frame: pd.DataFrame) -> str:
    """Hash normalized rows deterministically without persisting another file."""

    if frame.empty:
        return hashlib.sha256(b"").hexdigest()
    data = frame.copy()
    for column in ("bar_open_utc", "bar_close_utc"):
        if column in data:
            data[column] = pd.to_datetime(data[column], utc=True).map(_iso_utc)
    payload = data.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def extract_clean_cohort(
    outcomes_path: Path | str,
    *,
    cutoff_utc: str | datetime = DEFAULT_CUTOFF_UTC,
    expected_cohort_size: int | None = DEFAULT_EXPECTED_COHORT_SIZE,
) -> list[CohortRecord]:
    """Extract one unambiguous OTD/ORR record per signal from integrity-v2 data."""

    path = Path(outcomes_path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as error:  # noqa: BLE001
        raise CohortIntegrityError(f"Unable to read outcomes artifact: {path.name}") from error

    cutoff = _parse_utc(cutoff_utc, field_name="cutoff_utc")
    candidates: list[dict[str, Any]] = []

    for row in _walk_objects(payload):
        if row.get("tracking_scope") != RESEARCH_SCOPE:
            continue
        if row.get("scenario_family") not in COHORT_FAMILIES:
            continue
        if not row.get("signal_id"):
            continue

        lifecycle_value = (
            row.get("signal_created_at_utc")
            or row.get("source_event_ts_utc")
            or row.get("created_at_utc")
        )
        lifecycle_time = _parse_utc(
            lifecycle_value,
            field_name="signal_created_at_utc",
        )
        if lifecycle_time < cutoff:
            continue
        candidates.append(row)

    id_counts = Counter(str(row.get("signal_id")) for row in candidates)
    duplicates = sorted(signal_id for signal_id, count in id_counts.items() if count != 1)
    if duplicates:
        raise CohortIntegrityError(
            "Cohort contains duplicate signal_id records: " + ", ".join(duplicates)
        )

    records: list[CohortRecord] = []
    for row in candidates:
        signal_id = str(row["signal_id"])
        symbol = _safe_text(row.get("symbol")) or signal_id.split("_", 1)[0]
        if not symbol:
            raise CohortIntegrityError(f"Unable to resolve symbol for signal_id={signal_id}")

        signal_created = _parse_utc(
            row.get("signal_created_at_utc")
            or row.get("created_at_utc")
            or row.get("source_event_ts_utc"),
            field_name="signal_created_at_utc",
        )
        selected_evaluation = _parse_utc(
            row.get("source_event_ts_utc")
            or row.get("created_at_utc"),
            field_name="source_event_ts_utc",
        )
        expires_at = _parse_utc(
            row.get("expires_at_utc"),
            field_name="expires_at_utc",
        )

        if expires_at <= signal_created:
            raise CohortIntegrityError(
                f"Expiry must be after signal creation for signal_id={signal_id}"
            )
        if selected_evaluation < signal_created:
            raise CohortIntegrityError(
                f"Selected evaluation predates signal creation for signal_id={signal_id}"
            )

        evaluation_lag_minutes = round(
            (selected_evaluation - signal_created).total_seconds() / 60.0,
            4,
        )
        evaluation_within_lifecycle = selected_evaluation < expires_at
        evaluation_after_expiry_minutes = (
            None
            if evaluation_within_lifecycle
            else round(
                (selected_evaluation - expires_at).total_seconds() / 60.0,
                4,
            )
        )

        records.append(
            CohortRecord(
                signal_id=signal_id,
                symbol=symbol.upper(),
                scenario_family=str(row["scenario_family"]),
                scenario_type=_safe_text(row.get("scenario_type") or row.get("scenario")),
                direction=_safe_text(row.get("direction")),
                signal_created_at_utc=signal_created,
                decision_time_utc=signal_created,
                selected_evaluation_at_utc=selected_evaluation,
                selected_evaluation_within_original_lifecycle=(
                    evaluation_within_lifecycle
                ),
                selected_evaluation_lag_minutes=evaluation_lag_minutes,
                selected_evaluation_after_expiry_minutes=(
                    evaluation_after_expiry_minutes
                ),
                expires_at_utc=expires_at,
                htf_bias=_safe_text(row.get("htf_bias")),
                signal_alignment=_safe_text(row.get("signal_alignment")),
                macro_guard_status=_safe_text(row.get("macro_guard_status")),
                stop_quality=_safe_text(row.get("stop_quality")),
                battle_permission=_safe_text(row.get("battle_permission")),
                battle_permission_blockers=_safe_text_tuple(
                    row.get("battle_permission_blockers")
                ),
            )
        )

    records.sort(key=lambda item: (item.decision_time_utc, item.signal_id))
    if expected_cohort_size is not None and len(records) != expected_cohort_size:
        raise CohortIntegrityError(
            f"Expected {expected_cohort_size} clean OTD/ORR signals, found {len(records)}"
        )
    return records


def build_fetch_windows(
    records: Sequence[CohortRecord],
    *,
    lookback_days: int = DEFAULT_FETCH_LOOKBACK_DAYS,
) -> list[FetchWindow]:
    """Create one conservative bounding fetch window per symbol."""

    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    grouped: dict[str, list[CohortRecord]] = defaultdict(list)
    for record in records:
        grouped[record.symbol].append(record)

    windows: list[FetchWindow] = []
    lookback = timedelta(days=lookback_days)
    for symbol, symbol_records in sorted(grouped.items()):
        start = min(item.decision_time_utc - lookback for item in symbol_records)
        end = max(item.expires_at_utc for item in symbol_records)
        windows.append(FetchWindow(symbol=symbol, start_utc=start, end_utc=end))
    return windows


def split_fetch_window(
    window: FetchWindow,
    *,
    chunk_days: int = DEFAULT_PROVIDER_CHUNK_DAYS,
) -> list[FetchWindow]:
    if chunk_days <= 0:
        raise ValueError("chunk_days must be positive")
    chunks: list[FetchWindow] = []
    cursor = window.start_utc
    delta = timedelta(days=chunk_days)
    while cursor < window.end_utc:
        end = min(cursor + delta, window.end_utc)
        chunks.append(FetchWindow(window.symbol, cursor, end))
        cursor = end
    return chunks


def _flatten_provider_columns(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    if isinstance(data.columns, pd.MultiIndex):
        flattened: list[str] = []
        for column in data.columns:
            parts = [str(part) for part in column if str(part or "").strip()]
            preferred = next(
                (
                    part
                    for part in parts
                    if part.lower() in {"open", "high", "low", "close", "volume"}
                ),
                parts[0] if parts else "",
            )
            flattened.append(preferred)
        data.columns = flattened
    return data


def normalize_ohlc_frame(
    frame: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    source_provider: str,
) -> NormalizedBars:
    """Normalize OHLC and add explicit bar-open/bar-close timestamps."""

    if timeframe not in {"5m", "15m"}:
        raise ValueError(f"Unsupported research timeframe: {timeframe}")
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return NormalizedBars(
            frame=pd.DataFrame(columns=BAR_COLUMNS),
            duplicate_rows_discarded=0,
            invalid_rows_discarded=0,
        )

    data = _flatten_provider_columns(frame)
    lower_to_column = {str(column).lower(): column for column in data.columns}

    timestamp_source: Any
    if isinstance(data.index, pd.DatetimeIndex):
        timestamp_source = data.index
    else:
        timestamp_column = next(
            (
                lower_to_column[name]
                for name in ("datetime", "timestamp", "time", "date")
                if name in lower_to_column
            ),
            None,
        )
        if timestamp_column is None:
            raise CoverageError(f"{symbol} {timeframe} data has no timestamp column")
        timestamp_source = data[timestamp_column]

    output = pd.DataFrame(index=data.index)
    output["bar_open_utc"] = pd.to_datetime(timestamp_source, utc=True, errors="coerce")

    for required in ("open", "high", "low", "close"):
        source_column = lower_to_column.get(required)
        if source_column is None:
            raise CoverageError(f"{symbol} {timeframe} data is missing {required}")
        output[required] = pd.to_numeric(data[source_column], errors="coerce").to_numpy()

    volume_column = lower_to_column.get("volume")
    if volume_column is None:
        output["volume"] = 0.0
    else:
        output["volume"] = (
            pd.to_numeric(data[volume_column], errors="coerce").fillna(0.0).to_numpy()
        )

    before_drop = len(output)
    output = output.dropna(subset=["bar_open_utc", "open", "high", "low", "close"])
    invalid_rows = before_drop - len(output)

    duplicate_mask = output.duplicated(subset=["bar_open_utc"], keep="last")
    duplicate_rows = int(duplicate_mask.sum())
    output = output.loc[~duplicate_mask].copy()
    output = output.sort_values("bar_open_utc").reset_index(drop=True)

    minutes = 5 if timeframe == "5m" else 15
    output.insert(0, "symbol", symbol.upper())
    output.insert(1, "timeframe", timeframe)
    output.insert(
        3,
        "bar_close_utc",
        output["bar_open_utc"] + pd.Timedelta(minutes=minutes),
    )
    output["source_provider"] = source_provider
    output = output[BAR_COLUMNS]

    return NormalizedBars(
        frame=output,
        duplicate_rows_discarded=duplicate_rows,
        invalid_rows_discarded=invalid_rows,
    )


def _concat_normalized(
    frames: Sequence[pd.DataFrame],
) -> tuple[pd.DataFrame, int]:
    usable = [frame for frame in frames if isinstance(frame, pd.DataFrame) and not frame.empty]
    if not usable:
        return pd.DataFrame(columns=BAR_COLUMNS), 0
    combined = pd.concat(usable, ignore_index=True)
    duplicate_mask = combined.duplicated(
        subset=["symbol", "timeframe", "bar_open_utc"],
        keep="last",
    )
    overlap_duplicates = int(duplicate_mask.sum())
    combined = combined.loc[~duplicate_mask]
    combined = combined.sort_values(["symbol", "bar_open_utc"]).reset_index(drop=True)
    return combined[BAR_COLUMNS], overlap_duplicates


def _read_ohlc_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    raise CoverageError(f"Unsupported OHLC input type: {path.name}")


def _resolve_local_5m_file(source_dir: Path, symbol: str) -> Path:
    candidates = (
        source_dir / symbol / "5m.parquet",
        source_dir / f"{symbol}_5m.parquet",
        source_dir / symbol / "5m.csv",
        source_dir / f"{symbol}_5m.csv",
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise CoverageError(f"No local 5m OHLC file found for symbol={symbol}")


def load_cached_15m(
    cache_dir: Path | str,
    records: Sequence[CohortRecord],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    root = Path(cache_dir)
    frames: list[pd.DataFrame] = []
    audit_files: list[dict[str, Any]] = []
    source_duplicates = 0
    invalid_rows = 0

    for symbol in sorted({record.symbol for record in records}):
        path = root / symbol / "15m.parquet"
        if not path.is_file():
            raise CoverageError(f"Missing production 15m cache for symbol={symbol}")
        normalized = normalize_ohlc_frame(
            pd.read_parquet(path),
            symbol=symbol,
            timeframe="15m",
            source_provider="production_parquet_cache",
        )
        source_duplicates += normalized.duplicate_rows_discarded
        invalid_rows += normalized.invalid_rows_discarded
        frames.append(normalized.frame)
        audit_files.append(
            {
                "symbol": symbol,
                "file_name": path.name,
                "source_sha256": sha256_file(path),
                "normalized_rows": len(normalized.frame),
            }
        )

    combined, overlap_duplicates = _concat_normalized(frames)
    return combined, {
        "source": "production_parquet_cache",
        "source_files": audit_files,
        "source_duplicate_rows_discarded": source_duplicates,
        "overlap_duplicate_rows_discarded": overlap_duplicates,
        "invalid_rows_discarded": invalid_rows,
        "normalized_source_rows": len(combined),
        "normalized_source_sha256": sha256_frame(combined),
    }


def load_local_5m(
    source_dir: Path | str,
    records: Sequence[CohortRecord],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    root = Path(source_dir)
    frames: list[pd.DataFrame] = []
    audit_files: list[dict[str, Any]] = []
    source_duplicates = 0
    invalid_rows = 0

    for symbol in sorted({record.symbol for record in records}):
        path = _resolve_local_5m_file(root, symbol)
        normalized = normalize_ohlc_frame(
            _read_ohlc_file(path),
            symbol=symbol,
            timeframe="5m",
            source_provider="local_versioned_input",
        )
        source_duplicates += normalized.duplicate_rows_discarded
        invalid_rows += normalized.invalid_rows_discarded
        frames.append(normalized.frame)
        audit_files.append(
            {
                "symbol": symbol,
                "file_name": path.name,
                "source_sha256": sha256_file(path),
                "normalized_rows": len(normalized.frame),
            }
        )

    combined, overlap_duplicates = _concat_normalized(frames)
    return combined, {
        "source": "local_versioned_input",
        "source_files": audit_files,
        "source_duplicate_rows_discarded": source_duplicates,
        "overlap_duplicate_rows_discarded": overlap_duplicates,
        "invalid_rows_discarded": invalid_rows,
        "normalized_source_rows": len(combined),
        "normalized_source_sha256": sha256_frame(combined),
        "provider_requests": [],
    }


def _safe_provider_error(provider: str, symbol: str, error: BaseException) -> ProviderFetchError:
    return ProviderFetchError(
        f"{provider} 5m fetch failed for symbol={symbol}; error_type={type(error).__name__}"
    )


def _fetch_yfinance_window(
    window: FetchWindow,
    *,
    provider_cache_dir: Path,
) -> pd.DataFrame:
    ticker = YFINANCE_SYMBOLS.get(window.symbol)
    if ticker is None:
        raise ProviderFetchError(f"No yfinance research mapping for symbol={window.symbol}")
    try:
        import yfinance as yf

        provider_cache_dir.mkdir(parents=True, exist_ok=True)
        yf.set_tz_cache_location(str(provider_cache_dir))
        frame = yf.download(
            ticker,
            start=window.start_utc,
            end=window.end_utc + timedelta(minutes=5),
            interval="5m",
            progress=False,
            auto_adjust=False,
            threads=False,
            timeout=30,
        )
    except Exception as error:  # noqa: BLE001
        raise _safe_provider_error("yfinance", window.symbol, error) from None
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ProviderFetchError(f"yfinance returned no 5m rows for symbol={window.symbol}")
    return frame


def _fetch_twelvedata_window(
    window: FetchWindow,
    *,
    api_key_env: str,
    outputsize: int,
) -> pd.DataFrame:
    provider_symbol = TWELVEDATA_SYMBOLS.get(window.symbol)
    if provider_symbol is None:
        raise ProviderFetchError(f"No TwelveData research mapping for symbol={window.symbol}")

    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise ProviderFetchError(
            f"TwelveData credential is unavailable in configured environment variable for symbol={window.symbol}"
        )

    try:
        import requests

        response = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": provider_symbol,
                "interval": "5min",
                "start_date": window.start_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "end_date": window.end_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "timezone": "UTC",
                "order": "asc",
                "format": "JSON",
                "outputsize": outputsize,
                "apikey": api_key,
            },
            timeout=30,
        )
    except Exception as error:  # noqa: BLE001
        raise _safe_provider_error("TwelveData", window.symbol, error) from None

    if response.status_code != 200:
        raise ProviderFetchError(
            f"TwelveData 5m fetch failed for symbol={window.symbol}; http_status={response.status_code}"
        )
    try:
        payload = response.json()
    except Exception as error:  # noqa: BLE001
        raise _safe_provider_error("TwelveData", window.symbol, error) from None
    if str(payload.get("status") or "").lower() == "error":
        code = payload.get("code")
        raise ProviderFetchError(
            f"TwelveData 5m fetch failed for symbol={window.symbol}; api_code={code}"
        )
    values = payload.get("values")
    if not isinstance(values, list) or not values:
        raise ProviderFetchError(f"TwelveData returned no 5m rows for symbol={window.symbol}")
    return pd.DataFrame(values)


def fetch_live_5m(
    records: Sequence[CohortRecord],
    *,
    lookback_days: int = DEFAULT_FETCH_LOOKBACK_DAYS,
    chunk_days: int = DEFAULT_PROVIDER_CHUNK_DAYS,
    twelvedata_outputsize: int = DEFAULT_TWELVEDATA_OUTPUTSIZE,
    twelvedata_api_key_env: str = "TWELVEDATA_API_KEY",
    provider_pause_seconds: float = 8.0,
    provider_cache_dir: Path | str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Fetch 5m data without ever logging request URLs or credential values."""

    if provider_pause_seconds < 0:
        raise ValueError("provider_pause_seconds cannot be negative")
    cache_path = Path(provider_cache_dir or (Path(tempfile.gettempdir()) / "yfinance-cache"))
    frames: list[pd.DataFrame] = []
    request_audit: list[dict[str, Any]] = []
    source_duplicates = 0
    invalid_rows = 0
    requests_completed = 0

    for symbol_window in build_fetch_windows(records, lookback_days=lookback_days):
        provider = (
            "twelvedata"
            if symbol_window.symbol in TWELVEDATA_SYMBOLS
            else "yfinance"
            if symbol_window.symbol in YFINANCE_SYMBOLS
            else None
        )
        if provider is None:
            raise ProviderFetchError(f"No research provider mapping for symbol={symbol_window.symbol}")

        chunks = (
            split_fetch_window(symbol_window, chunk_days=chunk_days)
            if provider == "twelvedata"
            else [symbol_window]
        )
        for chunk in chunks:
            if requests_completed and provider_pause_seconds:
                time.sleep(provider_pause_seconds)
            if provider == "twelvedata":
                raw = _fetch_twelvedata_window(
                    chunk,
                    api_key_env=twelvedata_api_key_env,
                    outputsize=twelvedata_outputsize,
                )
            else:
                raw = _fetch_yfinance_window(chunk, provider_cache_dir=cache_path)

            normalized = normalize_ohlc_frame(
                raw,
                symbol=chunk.symbol,
                timeframe="5m",
                source_provider=provider,
            )
            source_duplicates += normalized.duplicate_rows_discarded
            invalid_rows += normalized.invalid_rows_discarded
            frames.append(normalized.frame)
            requests_completed += 1
            request_audit.append(
                {
                    "provider": provider,
                    "symbol": chunk.symbol,
                    "provider_symbol": (
                        TWELVEDATA_SYMBOLS.get(chunk.symbol)
                        if provider == "twelvedata"
                        else YFINANCE_SYMBOLS.get(chunk.symbol)
                    ),
                    "start_utc": _iso_utc(chunk.start_utc),
                    "end_utc": _iso_utc(chunk.end_utc),
                    "normalized_rows": len(normalized.frame),
                    "status": "OK",
                }
            )

    combined, overlap_duplicates = _concat_normalized(frames)
    return combined, {
        "source": "live_provider_fetch",
        "provider_requests": request_audit,
        "source_duplicate_rows_discarded": source_duplicates,
        "overlap_duplicate_rows_discarded": overlap_duplicates,
        "invalid_rows_discarded": invalid_rows,
        "normalized_source_rows": len(combined),
        "normalized_source_sha256": sha256_frame(combined),
    }


def _symbol_bars(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    return frame.loc[frame["symbol"] == symbol].sort_values("bar_open_utc")


def _select_record_window(
    frame: pd.DataFrame,
    record: CohortRecord,
    *,
    timeframe: str,
    pre_event_bars: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if timeframe not in {"5m", "15m"}:
        raise ValueError(f"Unsupported research timeframe: {timeframe}")
    symbol_frame = _symbol_bars(frame, record.symbol)
    decision = pd.Timestamp(record.decision_time_utc)
    expiry = pd.Timestamp(record.expires_at_utc)

    pre = symbol_frame.loc[symbol_frame["bar_close_utc"] <= decision].tail(pre_event_bars)
    forward = symbol_frame.loc[
        (symbol_frame["bar_close_utc"] > decision)
        & (symbol_frame["bar_close_utc"] <= expiry)
    ]
    selected = pd.concat([pre, forward], ignore_index=True)
    selected = selected.drop_duplicates(subset=["symbol", "timeframe", "bar_open_utc"])
    selected = selected.sort_values("bar_open_utc")

    interval_minutes = 5 if timeframe == "5m" else 15
    if selected.empty:
        gaps = pd.Series(dtype=float)
    else:
        gaps = (
            pd.to_datetime(selected["bar_open_utc"], utc=True)
            .sort_values()
            .diff()
            .dropna()
            .dt.total_seconds()
            / 60.0
        )
    large_gaps = gaps.loc[gaps > interval_minutes * 1.5]

    coverage = {
        "pre_event_closed_bars": len(pre),
        "required_pre_event_closed_bars": pre_event_bars,
        "forward_closed_bars_before_expiry": len(forward),
        "first_selected_bar_open_utc": (
            _iso_utc(selected["bar_open_utc"].min()) if not selected.empty else None
        ),
        "last_selected_bar_close_utc": (
            _iso_utc(selected["bar_close_utc"].max()) if not selected.empty else None
        ),
        "max_gap_minutes": round(float(gaps.max()), 4) if not gaps.empty else 0.0,
        "gaps_over_1_5_intervals": int(len(large_gaps)),
        "complete": len(pre) == pre_event_bars and len(forward) > 0,
    }
    return selected, coverage


def select_replay_bars(
    records: Sequence[CohortRecord],
    bars_15m: pd.DataFrame,
    bars_5m: pd.DataFrame,
    *,
    pre_event_bars: int = DEFAULT_PRE_EVENT_BARS,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    """Select closed pre-event bars plus bars closing before original expiry."""

    if pre_event_bars <= 0:
        raise ValueError("pre_event_bars must be positive")

    selected_15m: list[pd.DataFrame] = []
    selected_5m: list[pd.DataFrame] = []
    coverage_rows: list[dict[str, Any]] = []
    incomplete: list[str] = []

    for record in records:
        frame_15m, audit_15m = _select_record_window(
            bars_15m,
            record,
            timeframe="15m",
            pre_event_bars=pre_event_bars,
        )
        frame_5m, audit_5m = _select_record_window(
            bars_5m,
            record,
            timeframe="5m",
            pre_event_bars=pre_event_bars,
        )
        selected_15m.append(frame_15m)
        selected_5m.append(frame_5m)
        complete = bool(audit_15m["complete"] and audit_5m["complete"])
        if not complete:
            incomplete.append(record.signal_id)
        coverage_rows.append(
            {
                "signal_id": record.signal_id,
                "symbol": record.symbol,
                "decision_time_utc": _iso_utc(record.decision_time_utc),
                "expires_at_utc": _iso_utc(record.expires_at_utc),
                "coverage_complete": complete,
                "coverage_15m": audit_15m,
                "coverage_5m": audit_5m,
            }
        )

    if incomplete:
        raise CoverageError(
            "Incomplete closed-bar coverage for signal_id values: " + ", ".join(incomplete)
        )

    combined_15m, _ = _concat_normalized(selected_15m)
    combined_5m, _ = _concat_normalized(selected_5m)
    return combined_15m, combined_5m, coverage_rows


def add_selection_audit(
    audit: Mapping[str, Any],
    *,
    timeframe: str,
    selected_rows: int,
    coverage: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Record benign overlap deduplication across per-signal replay windows."""

    key = f"coverage_{timeframe}"
    before_dedup = sum(
        int(row[key]["pre_event_closed_bars"])
        + int(row[key]["forward_closed_bars_before_expiry"])
        for row in coverage
    )
    result = dict(audit)
    result.update(
        {
            "selected_window_rows_before_dedup": before_dedup,
            "selected_unique_rows": selected_rows,
            "selection_overlap_duplicate_rows_discarded": before_dedup - selected_rows,
            "selected_rows_sha256": None,
        }
    )
    return result


def _assert_no_sensitive_keys(value: Any, path: str = "root") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).lower()
            if any(fragment in normalized for fragment in SENSITIVE_KEY_FRAGMENTS):
                raise BackfillError(f"Sensitive key is forbidden in artifact metadata: {path}.{key}")
            _assert_no_sensitive_keys(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_no_sensitive_keys(child, f"{path}[{index}]")


def _json_write(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def write_versioned_artifact(
    output_dir: Path | str,
    *,
    outcomes_path: Path,
    records: Sequence[CohortRecord],
    bars_15m: pd.DataFrame,
    bars_5m: pd.DataFrame,
    coverage: Sequence[dict[str, Any]],
    audit_15m: Mapping[str, Any],
    audit_5m: Mapping[str, Any],
    dry_run: bool = False,
) -> dict[str, Any]:
    destination = Path(output_dir)
    if destination.exists():
        raise FileExistsError(f"Versioned output already exists: {destination}")

    family_counts = Counter(record.scenario_family for record in records)
    direction_counts = Counter(str(record.direction) for record in records)
    evaluations_within_lifecycle = sum(
        record.selected_evaluation_within_original_lifecycle for record in records
    )
    evaluations_at_or_after_expiry = len(records) - evaluations_within_lifecycle
    manifest: dict[str, Any] = {
        "version": BACKFILL_VERSION,
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "artifact_mode": ARTIFACT_MODE,
        "research_only": True,
        "executable_signal": False,
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "source_outcomes_file": outcomes_path.name,
        "source_outcomes_sha256": sha256_file(outcomes_path),
        "cohort": {
            "signals": len(records),
            "scenario_families": dict(sorted(family_counts.items())),
            "directions": dict(sorted(direction_counts.items())),
            "decision_time_source": "signal_created_at_utc",
            "selected_evaluation_audit": {
                "within_original_lifecycle": evaluations_within_lifecycle,
                "at_or_after_original_expiry": evaluations_at_or_after_expiry,
                "context_use_rule": (
                    "selected evaluation metadata is audit-only and must not be "
                    "used before selected_evaluation_at_utc; an evaluation at or "
                    "after expiry is never replay-eligible context"
                ),
            },
        },
        "bar_availability_rule": {
            "decision_time_source": "signal_created_at_utc",
            "timestamp_semantics": "bar_open_utc",
            "5m_available_at": "bar_open_utc + 5 minutes",
            "15m_available_at": "bar_open_utc + 15 minutes",
            "pre_event_filter": "bar_close_utc <= decision_time_utc",
            "forward_filter": "decision_time_utc < bar_close_utc <= expires_at_utc",
            "look_ahead_allowed": False,
        },
        "rows": {
            "bars_5m": len(bars_5m),
            "bars_15m": len(bars_15m),
        },
        "coverage": list(coverage),
        "audit_15m": dict(audit_15m),
        "audit_5m": dict(audit_5m),
        "files": {},
        "notes": [
            "research_only",
            "non_executable",
            "does_not_mutate_battle_ready",
            "does_not_mutate_telegram_delivery_mode",
            "does_not_overwrite_runtime_or_cache",
        ],
    }
    _assert_no_sensitive_keys(manifest)

    if dry_run:
        return {
            "status": "DRY_RUN_OK",
            "output_created": False,
            "output_dir": str(destination),
            "manifest": manifest,
        }

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(
            prefix=f".{destination.name}.tmp-",
            dir=str(destination.parent),
        )
    )
    try:
        cohort_path = temporary / "cohort.json"
        bars_5m_path = temporary / "bars_5m.parquet"
        bars_15m_path = temporary / "bars_15m.parquet"
        manifest_path = temporary / "manifest.json"
        checksums_path = temporary / "checksums.sha256"

        _json_write(cohort_path, [record.to_dict() for record in records])
        bars_5m.to_parquet(bars_5m_path, index=False)
        bars_15m.to_parquet(bars_15m_path, index=False)

        manifest["files"] = {
            "cohort.json": {
                "sha256": sha256_file(cohort_path),
                "records": len(records),
            },
            "bars_5m.parquet": {
                "sha256": sha256_file(bars_5m_path),
                "rows": len(bars_5m),
            },
            "bars_15m.parquet": {
                "sha256": sha256_file(bars_15m_path),
                "rows": len(bars_15m),
            },
        }
        _assert_no_sensitive_keys(manifest)
        _json_write(manifest_path, manifest)

        checksum_entries = []
        for path in (cohort_path, bars_5m_path, bars_15m_path, manifest_path):
            checksum_entries.append(f"{sha256_file(path)}  {path.name}")
        checksums_path.write_text("\n".join(checksum_entries) + "\n", encoding="utf-8")

        temporary.rename(destination)
    except Exception:
        # The temporary directory is exclusively created by this builder.  Keep
        # production cache/runtime untouched even when artifact creation fails.
        for child in temporary.glob("*"):
            if child.is_file():
                child.unlink()
        temporary.rmdir()
        raise

    return {
        "status": "OK",
        "output_created": True,
        "output_dir": str(destination),
        "cohort_signals": len(records),
        "bars_5m": len(bars_5m),
        "bars_15m": len(bars_15m),
        "manifest_sha256": sha256_file(destination / "manifest.json"),
    }


def build_dataset(
    *,
    outcomes_path: Path | str,
    cache_dir: Path | str,
    output_dir: Path | str,
    five_minute_source: str,
    five_minute_source_dir: Path | str | None = None,
    cutoff_utc: str = DEFAULT_CUTOFF_UTC,
    expected_cohort_size: int | None = DEFAULT_EXPECTED_COHORT_SIZE,
    pre_event_bars: int = DEFAULT_PRE_EVENT_BARS,
    fetch_lookback_days: int = DEFAULT_FETCH_LOOKBACK_DAYS,
    allow_network_fetch: bool = False,
    provider_pause_seconds: float = 8.0,
    provider_cache_dir: Path | str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    outcomes = Path(outcomes_path)
    records = extract_clean_cohort(
        outcomes,
        cutoff_utc=cutoff_utc,
        expected_cohort_size=expected_cohort_size,
    )
    bars_15m_all, audit_15m = load_cached_15m(cache_dir, records)

    source_mode = five_minute_source.strip().lower()
    if source_mode == "directory":
        if five_minute_source_dir is None:
            raise ValueError("five_minute_source_dir is required for directory mode")
        bars_5m_all, audit_5m = load_local_5m(five_minute_source_dir, records)
    elif source_mode == "live":
        if not allow_network_fetch:
            raise BackfillError("Live provider retrieval requires --allow-network-fetch")
        bars_5m_all, audit_5m = fetch_live_5m(
            records,
            lookback_days=fetch_lookback_days,
            provider_pause_seconds=provider_pause_seconds,
            provider_cache_dir=provider_cache_dir,
        )
    else:
        raise ValueError("five_minute_source must be 'directory' or 'live'")

    bars_15m, bars_5m, coverage = select_replay_bars(
        records,
        bars_15m_all,
        bars_5m_all,
        pre_event_bars=pre_event_bars,
    )
    audit_15m = add_selection_audit(
        audit_15m,
        timeframe="15m",
        selected_rows=len(bars_15m),
        coverage=coverage,
    )
    audit_15m["selected_rows_sha256"] = sha256_frame(bars_15m)
    audit_5m = add_selection_audit(
        audit_5m,
        timeframe="5m",
        selected_rows=len(bars_5m),
        coverage=coverage,
    )
    audit_5m["selected_rows_sha256"] = sha256_frame(bars_5m)
    return write_versioned_artifact(
        output_dir,
        outcomes_path=outcomes,
        records=records,
        bars_15m=bars_15m,
        bars_5m=bars_5m,
        coverage=coverage,
        audit_15m=audit_15m,
        audit_5m=audit_5m,
        dry_run=dry_run,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a versioned research-only OTD/ORR 5m+15m dataset.",
    )
    parser.add_argument("--outcomes", required=True, type=Path)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--five-minute-source",
        required=True,
        choices=("directory", "live"),
    )
    parser.add_argument("--five-minute-source-dir", type=Path)
    parser.add_argument("--cutoff-utc", default=DEFAULT_CUTOFF_UTC)
    parser.add_argument(
        "--expected-cohort-size",
        default=DEFAULT_EXPECTED_COHORT_SIZE,
        type=int,
    )
    parser.add_argument("--pre-event-bars", default=DEFAULT_PRE_EVENT_BARS, type=int)
    parser.add_argument(
        "--fetch-lookback-days",
        default=DEFAULT_FETCH_LOOKBACK_DAYS,
        type=int,
    )
    parser.add_argument("--allow-network-fetch", action="store_true")
    parser.add_argument("--provider-pause-seconds", default=8.0, type=float)
    parser.add_argument("--provider-cache-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_dataset(
        outcomes_path=args.outcomes,
        cache_dir=args.cache_dir,
        output_dir=args.output_dir,
        five_minute_source=args.five_minute_source,
        five_minute_source_dir=args.five_minute_source_dir,
        cutoff_utc=args.cutoff_utc,
        expected_cohort_size=args.expected_cohort_size,
        pre_event_bars=args.pre_event_bars,
        fetch_lookback_days=args.fetch_lookback_days,
        allow_network_fetch=args.allow_network_fetch,
        provider_pause_seconds=args.provider_pause_seconds,
        provider_cache_dir=args.provider_cache_dir,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
