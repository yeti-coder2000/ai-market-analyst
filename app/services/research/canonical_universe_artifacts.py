from __future__ import annotations

"""Deterministic identities and research-only canonical bundle utilities."""

import gzip
import hashlib
import json
import subprocess
import zipfile
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

EVENT_ID = "candidate_id"
CANONICAL_ZIP_NAME = "AI_Market_Analyst_Canonical_Frozen_Universe_v2.zip"


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
) -> dict[str, Any]:
    columns = sorted(str(column) for column in frame.columns)
    rows = [
        {column: _json_value(row.get(column)) for column in columns}
        for row in frame.to_dict(orient="records")
    ]
    semantic = json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=False)
    schema = [(column, str(frame[column].dtype)) for column in columns]
    ids = [str(value) for value in frame.get(id_column, pd.Series(dtype=str)).tolist()]
    timestamps = pd.to_datetime(frame.get(timestamp_column), utc=True, errors="coerce")
    return {
        "semantic_content_sha256": hashlib.sha256(semantic.encode()).hexdigest(),
        "schema_hash": hashlib.sha256(repr(schema).encode()).hexdigest(),
        "ordered_id_hash": hashlib.sha256(("\n".join(ids) + "\n").encode()).hexdigest(),
        "row_count": len(frame),
        "unique_id_count": len(set(ids)),
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
        json.dumps(_json_value(value), indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    temporary.replace(path)


def write_compact_csv_gz(
    path: Path, frame: pd.DataFrame, columns: Sequence[str]
) -> None:
    selected = frame.reindex(columns=list(columns))
    payload = selected.to_csv(index=False, lineterminator="\n").encode()
    temporary = path.with_suffix(path.suffix + ".tmp")
    with (
        temporary.open("wb") as raw,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped,
    ):
        zipped.write(payload)
    temporary.replace(path)


def outcome_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    tp = sum(str(row.get("outcome")) == "TP_HIT" for row in rows)
    sl = sum(str(row.get("outcome")) == "SL_HIT" for row in rows)
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
    run_dir: Path, files: Sequence[Path], *, universe_frozen: bool
) -> Path:
    if not universe_frozen:
        raise ValueError(
            "canonical ZIP name is reserved for a frozen 13-asset universe"
        )
    destination = run_dir / CANONICAL_ZIP_NAME
    temporary = destination.with_suffix(".zip.tmp")
    inventory = [
        {"path": path.name, "sha256": sha256_file(path), "bytes": path.stat().st_size}
        for path in files
    ]
    with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(files, key=lambda item: item.name):
            archive.write(path, path.name)
        archive.writestr(
            "zip_inventory.json", json.dumps(inventory, indent=2, sort_keys=True) + "\n"
        )
    with zipfile.ZipFile(temporary) as archive:
        if archive.testzip() is not None:
            raise ValueError("ZIP restore verification failed")
    temporary.replace(destination)
    return destination
