from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

from .collectors.crypto_derivatives_collector import (
    DEFAULT_SNAPSHOT_FILENAME,
    build_crypto_manual_feed_payload,
    load_crypto_snapshot,
)
from .positioning_feed_merger import merge_positioning_feeds, write_merged_feed
from .positioning_service import build_daily_positioning_context
from .positioning_store import (
    get_manual_feed_path,
    get_positioning_dir,
    read_json_file,
    write_json_atomic,
)


POSITIONING_PIPELINE_VERSION = "positioning-pipeline-v0.1-runtime-orchestration"
CRYPTO_FEED_FILENAME = "crypto_daily_positioning_feed.json"
PIPELINE_HEALTH_FILENAME = "positioning_pipeline_health.json"


def refresh_positioning_runtime(
    runtime_dir: str | None = None,
    report_date: str | None = None,
) -> dict[str, Any]:
    """
    Refresh Positioning Intelligence runtime artifacts from currently available
    local sources.

    Sources in v0.1:
    - offline crypto derivatives snapshot, when present;
    - manual daily positioning feed, when present.

    The function is fail-open and research-only. It cannot allow or block a
    signal and cannot modify Battle Gate.
    """

    target_date = str(report_date or date.today().isoformat())
    positioning_dir = get_positioning_dir(runtime_dir)
    positioning_dir.mkdir(parents=True, exist_ok=True)

    manual_feed_path = get_manual_feed_path(runtime_dir=runtime_dir)
    crypto_snapshot_path = positioning_dir / DEFAULT_SNAPSHOT_FILENAME
    crypto_feed_path = positioning_dir / CRYPTO_FEED_FILENAME
    health_path = positioning_dir / PIPELINE_HEALTH_FILENAME

    feed_paths: list[Path] = []
    sources: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []

    # Automatic baseline first. Manual feed is appended second and therefore
    # remains an explicit operator override for duplicate symbols.
    _prepare_crypto_source(
        snapshot_path=crypto_snapshot_path,
        output_path=crypto_feed_path,
        target_date=target_date,
        feed_paths=feed_paths,
        sources=sources,
        errors=errors,
    )
    _prepare_manual_source(
        path=manual_feed_path,
        feed_paths=feed_paths,
        sources=sources,
        errors=errors,
    )

    usable_sources = [source for source in sources if int(source.get("items") or 0) > 0]
    available_sources = [source for source in sources if source.get("status") in {"OK", "EMPTY"}]

    if len(usable_sources) >= 2 and not errors:
        status_hint = "OK"
    elif usable_sources:
        status_hint = "PARTIAL"
    elif errors:
        status_hint = "ERROR"
    else:
        status_hint = "NO_DATA"

    if not manual_feed_path.exists():
        warnings.append(f"missing_optional_source:{manual_feed_path}")
    if not crypto_snapshot_path.exists():
        warnings.append(f"missing_optional_source:{crypto_snapshot_path}")

    merged = merge_positioning_feeds(
        feed_paths=feed_paths,
        target_date=target_date,
        dedupe_by_symbol=True,
        source_priority=None,
    )
    merged["pipeline_meta"] = {
        "version": POSITIONING_PIPELINE_VERSION,
        "report_date": target_date,
        "runtime_dir": str(positioning_dir.parent),
        "status_hint": status_hint,
        "sources_expected": ["crypto_snapshot", "manual_feed"],
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
        persist=True,
        fallback_date=target_date,
    )

    result = {
        "version": POSITIONING_PIPELINE_VERSION,
        "ok": str(snapshot.get("status") or "") not in {"ERROR"},
        "status": snapshot.get("status"),
        "report_date": target_date,
        "runtime_dir": str(positioning_dir.parent),
        "positioning_dir": str(positioning_dir),
        "merged_feed": str(merged_path),
        "latest_snapshot": str(positioning_dir / "daily_positioning_latest.json"),
        "sources": sources,
        "errors": errors,
        "warnings": warnings,
        "items": len(snapshot.get("items") or []),
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }
    write_json_atomic(health_path, result)
    return result


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
        write_json_atomic(output_path, payload)
        item_count = len(payload.get("items") or [])
        feed_paths.append(output_path)
        sources.append(
            {
                "name": "crypto_snapshot",
                "status": "OK" if item_count else "EMPTY",
                "path": str(snapshot_path),
                "feed_path": str(output_path),
                "source_date": snapshot.get("date"),
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


def _prepare_manual_source(
    *,
    path: Path,
    feed_paths: list[Path],
    sources: list[dict[str, Any]],
    errors: list[str],
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh Positioning Intelligence runtime artifacts.")
    parser.add_argument("--runtime-dir", default=None, help="Runtime directory.")
    parser.add_argument("--date", default=None, help="Report date YYYY-MM-DD.")
    args = parser.parse_args()

    result = refresh_positioning_runtime(
        runtime_dir=args.runtime_dir,
        report_date=args.date,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") != "ERROR" else 1


if __name__ == "__main__":
    raise SystemExit(main())
