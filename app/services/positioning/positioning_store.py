from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .positioning_models import PositioningSnapshot, utc_now_iso


DEFAULT_RUNTIME_DIR = "/var/data/runtime"
POSITIONING_DIR_NAME = "positioning"

MANUAL_FEED_FILENAME = "manual_daily_positioning_feed.json"
LATEST_FILENAME = "daily_positioning_latest.json"
HISTORY_FILENAME = "daily_positioning_history.jsonl"
SOURCE_HEALTH_FILENAME = "source_health.json"


def get_runtime_dir(runtime_dir: str | None = None) -> Path:
    base = runtime_dir or os.getenv("POSITIONING_RUNTIME_DIR") or os.getenv("RUNTIME_DIR") or DEFAULT_RUNTIME_DIR
    return Path(base)


def get_positioning_dir(runtime_dir: str | None = None) -> Path:
    return get_runtime_dir(runtime_dir) / POSITIONING_DIR_NAME


def get_manual_feed_path(runtime_dir: str | None = None, feed_path: str | None = None) -> Path:
    if feed_path:
        return Path(feed_path)
    env_path = os.getenv("POSITIONING_MANUAL_FEED_PATH")
    if env_path:
        return Path(env_path)
    return get_positioning_dir(runtime_dir) / MANUAL_FEED_FILENAME


def get_latest_path(runtime_dir: str | None = None) -> Path:
    return get_positioning_dir(runtime_dir) / LATEST_FILENAME


def get_history_path(runtime_dir: str | None = None) -> Path:
    return get_positioning_dir(runtime_dir) / HISTORY_FILENAME


def get_source_health_path(runtime_dir: str | None = None) -> Path:
    return get_positioning_dir(runtime_dir) / SOURCE_HEALTH_FILENAME


def ensure_positioning_dir(runtime_dir: str | None = None) -> Path:
    directory = get_positioning_dir(runtime_dir)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def read_json_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
    tmp_path.replace(path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        fh.write("\n")


def save_snapshot(snapshot: PositioningSnapshot, runtime_dir: str | None = None) -> None:
    ensure_positioning_dir(runtime_dir)
    payload = snapshot.to_dict()
    write_json_atomic(get_latest_path(runtime_dir), payload)
    append_jsonl(get_history_path(runtime_dir), payload)


def save_source_health(source_health: dict[str, Any], runtime_dir: str | None = None) -> None:
    ensure_positioning_dir(runtime_dir)
    payload = {
        "generated_at": utc_now_iso(),
        **source_health,
    }
    write_json_atomic(get_source_health_path(runtime_dir), payload)


def load_latest_snapshot(runtime_dir: str | None = None) -> dict[str, Any] | None:
    path = get_latest_path(runtime_dir)
    if not path.exists():
        return None
    return read_json_file(path)
