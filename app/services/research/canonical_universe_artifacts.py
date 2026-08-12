from __future__ import annotations

"""Deterministic identities and research-only canonical bundle utilities."""

import gzip
import hashlib
import json
import subprocess
import tempfile
import zipfile
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

EVENT_ID = "candidate_id"
CANONICAL_ZIP_NAME = "AI_Market_Analyst_Canonical_Frozen_Universe_v2.zip"
APPROVED_SYMBOLS = (
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "AUDUSD",
    "BTCUSD",
    "ETHUSD",
    "GER40",
    "NAS100",
    "SPX500",
    "UKOIL",
)


def code_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_value(value: Any) -> Any:
    if value is None or value is pd.NA or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        stamp = pd.Timestamp(value)
        stamp = (
            stamp.tz_localize("UTC")
            if stamp.tzinfo is None
            else stamp.tz_convert("UTC")
        )
        return stamp.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(nested) for key, nested in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_json_value(nested) for nested in value]
    if hasattr(value, "item"):
        return value.item()
    return value


def semantic_identity(
    frame: pd.DataFrame,
    *,
    id_column: str,
    timestamp_column: str,
    code_revision: str,
    environment_versions: Mapping[str, str],
    source_hashes: Sequence[str],
    data_cutoff_utc: str,
    holdout_cutoff_utc: str,
    sort_columns: Sequence[str] | None = None,
    canonical_columns: Sequence[str] | None = None,
) -> dict[str, Any]:
    if id_column not in frame.columns or timestamp_column not in frame.columns:
        raise ValueError("identity and timestamp columns are required")
    columns = list(canonical_columns or sorted(str(column) for column in frame.columns))
    if set(columns) != set(frame.columns) or len(columns) != len(frame.columns):
        raise ValueError("canonical columns must contain every column exactly once")
    order = list(sort_columns or (timestamp_column, id_column))
    if any(column not in frame.columns for column in order):
        raise ValueError("deterministic sort columns are missing")
    ids = frame[id_column]
    if ids.isna().any() or ids.astype(str).str.strip().eq("").any():
        raise ValueError("canonical IDs must be non-null and non-empty")
    if ids.astype(str).duplicated().any():
        raise ValueError("canonical IDs must be unique")
    timestamps = pd.to_datetime(frame[timestamp_column], utc=True, errors="coerce")
    if timestamps.isna().any():
        raise ValueError("all primary timestamps must parse")
    ordered = (
        frame.assign(_canonical_timestamp=timestamps)
        .sort_values(
            [
                "_canonical_timestamp",
                *[column for column in order if column != timestamp_column],
            ],
            kind="stable",
        )
        .drop(columns="_canonical_timestamp")
    )
    rows = [
        {column: _json_value(row.get(column)) for column in columns}
        for row in ordered.to_dict(orient="records")
    ]
    semantic = json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=False)
    schema = [(column, str(frame[column].dtype)) for column in columns]
    ordered_ids = [str(value) for value in ordered[id_column].tolist()]
    return {
        "semantic_content_sha256": hashlib.sha256(semantic.encode()).hexdigest(),
        "schema_hash": hashlib.sha256(repr(schema).encode()).hexdigest(),
        "ordered_id_hash": hashlib.sha256(
            ("\n".join(ordered_ids) + "\n").encode()
        ).hexdigest(),
        "row_count": len(frame),
        "unique_id_count": len(set(ordered_ids)),
        "min_timestamp": timestamps.min().isoformat()
        if timestamps is not None and timestamps.notna().any()
        else None,
        "max_timestamp": timestamps.max().isoformat()
        if timestamps is not None and timestamps.notna().any()
        else None,
        "code_sha": code_revision,
        "environment_versions": dict(environment_versions),
        "source_hashes": sorted(source_hashes),
        "data_cutoff_utc": data_cutoff_utc,
        "holdout_cutoff_utc": holdout_cutoff_utc,
    }


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(_json_value(value), indent=2, sort_keys=True, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(path)


def write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temporary, index=False, lineterminator="\n", encoding="utf-8")
    temporary.replace(path)


def write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def write_compact_csv_gz(
    path: Path, frame: pd.DataFrame, columns: Sequence[str]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    selected = frame.reindex(columns=list(columns))
    payload = selected.to_csv(index=False, lineterminator="\n").encode("utf-8")
    temporary = path.with_suffix(path.suffix + ".tmp")
    with (
        temporary.open("wb") as raw,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped,
    ):
        zipped.write(payload)
    temporary.replace(path)


def outcome_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    tp = sum(str(row.get("outcome")) == "TP_HIT" for row in rows)
    sl = sum(str(row.get("outcome")).startswith("SL_HIT") for row in rows)
    resolved = tp + sl
    result = {
        "tp_count": tp,
        "sl_count": sl,
        "resolved_count": resolved,
        "win_rate": tp / resolved if resolved else None,
    }
    validate_outcome_counts(result)
    return result


def validate_outcome_counts(value: Mapping[str, Any]) -> None:
    tp, sl, resolved = (
        int(value["tp_count"]),
        int(value["sl_count"]),
        int(value["resolved_count"]),
    )
    if resolved != tp + sl:
        raise ValueError("resolved_count must equal tp_count + sl_count")
    expected = tp / resolved if resolved else None
    if value.get("win_rate") != expected:
        raise ValueError("win_rate must equal tp_count / resolved_count")


def build_verified_zip(
    run_dir: Path, files: Sequence[Path], *, frozen_manifest: Mapping[str, Any]
) -> Path:
    required_proofs = (
        "provider_parity_approved",
        "cache_source_integrity_passed",
        "canonical_datasets_valid",
        "hashes_valid",
        "deterministic_rerun_passed",
        "restore_preconditions_passed",
    )
    if (
        frozen_manifest.get("bundle_type") != "CANONICAL_FULL_UNIVERSE"
        or tuple(frozen_manifest.get("symbols") or ()) != APPROVED_SYMBOLS
        or not all(frozen_manifest.get(key) is True for key in required_proofs)
    ):
        raise ValueError(
            "frozen-universe manifest does not prove canonical publication"
        )
    destination = run_dir / CANONICAL_ZIP_NAME
    temporary = destination.with_suffix(".zip.tmp")
    relative = [path.relative_to(run_dir).as_posix() for path in files]
    if len(relative) != len(set(relative)):
        raise ValueError("ZIP relative paths must be unique")
    inventory = [
        {"path": name, "sha256": sha256_file(path), "bytes": path.stat().st_size}
        for name, path in zip(relative, files, strict=True)
    ]
    with zipfile.ZipFile(
        temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        for name, path in sorted(zip(relative, files, strict=True)):
            info = zipfile.ZipInfo(name, (2026, 1, 1, 0, 0, 0))
            info.external_attr = 0o100644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(
                info,
                path.read_bytes(),
                compress_type=zipfile.ZIP_DEFLATED,
                compresslevel=9,
            )
        archive.writestr(
            "zip_inventory.json", json.dumps(inventory, indent=2, sort_keys=True) + "\n"
        )
    with tempfile.TemporaryDirectory() as root:
        with zipfile.ZipFile(temporary) as archive:
            archive.extractall(root)
        restored = Path(root)
        actual = {
            path.relative_to(restored).as_posix()
            for path in restored.rglob("*")
            if path.is_file()
        }
        expected = {*relative, "zip_inventory.json"}
        if actual != expected:
            raise ValueError("ZIP restore allowlist mismatch")
        for item in inventory:
            if sha256_file(restored / item["path"]) != item["sha256"]:
                raise ValueError("ZIP restore member hash mismatch")
        for parquet in (
            "otd_orr_event_census_v2.parquet",
            "execution_candidates_v2.parquet",
        ):
            matches = [path for path in restored.rglob(parquet)]
            if len(matches) != 1:
                raise ValueError("ZIP restore canonical Parquet missing")
            pd.read_parquet(matches[0])
    temporary.replace(destination)
    return destination
