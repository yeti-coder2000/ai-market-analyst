from __future__ import annotations

"""Fail-closed Python 3.12 research environment preflight."""

import argparse
import importlib
import platform
import shutil
import sys
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.research.canonical_universe_artifacts import write_json

PACKAGES = (
    "numpy",
    "pandas",
    "pyarrow",
    "requests",
    "dateutil",
    "pytz",
    "pydantic",
    "yfinance",
)


def check_environment(output_root: Path, cache_root: Path) -> dict[str, object]:
    if sys.version_info[:2] != (3, 12):
        raise RuntimeError(
            f"Python 3.12.x is required; found {platform.python_version()}"
        )
    versions = {}
    for name in PACKAGES:
        module = importlib.import_module(name)
        versions[name] = str(getattr(module, "__version__", "UNKNOWN"))
    if int(versions["numpy"].split(".")[0]) >= 2:
        raise RuntimeError("NumPy major version must be less than 2")
    if versions["pandas"] != "2.2.3" or versions["pyarrow"] != "15.0.2":
        raise RuntimeError("pandas==2.2.3 and pyarrow==15.0.2 are required")
    roots = {}
    for label, root in (("output", output_root), ("cache", cache_root)):
        root.mkdir(parents=True, exist_ok=True)
        probe = root / ".research_environment_probe.parquet"
        expected = pd.DataFrame(
            {"value": [1], "stamp": [pd.Timestamp("2026-01-01", tz="UTC")]}
        )
        expected.to_parquet(probe, index=False)
        actual = pd.read_parquet(probe)
        probe.unlink()
        if not expected.equals(actual):
            raise RuntimeError(f"Parquet round-trip failed for {label} root")
        usage = shutil.disk_usage(root)
        roots[label] = {
            "root": str(root.resolve()),
            "writable": True,
            "free_bytes": usage.free,
            "total_bytes": usage.total,
        }
    return {
        "status": "OK",
        "python_version": platform.python_version(),
        "python_required": "3.12.x",
        "versions": versions,
        "parquet_round_trip": "PASS",
        "roots": roots,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--manifest-path", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest = check_environment(args.output_root, args.cache_root)
    write_json(args.manifest_path, manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
