from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.services.edge_dashboard import EdgeDashboardService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build EDGE DASHBOARD v1/v2/v3 from radar_snapshot_v2.ndjson and radar_journal.ndjson"
    )
    parser.add_argument(
        "--runtime-dir",
        type=str,
        default=None,
        help="Path to runtime directory. Example: /var/data/runtime or ./runtime",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Path to output directory. Example: /var/data/runtime/edge_dashboard",
    )
    parser.add_argument(
        "--print-summary",
        action="store_true",
        help="Print main JSON summary to stdout",
    )
    parser.add_argument(
        "--print-scenario-edge",
        action="store_true",
        help="Print scenario edge summary JSON to stdout",
    )
    parser.add_argument(
        "--print-time-edge",
        action="store_true",
        help="Print time edge summary JSON to stdout",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    runtime_dir = Path(args.runtime_dir) if args.runtime_dir else None
    output_dir = Path(args.output_dir) if args.output_dir else None

    service = EdgeDashboardService(
        runtime_dir=runtime_dir,
        output_dir=output_dir,
    )
    result = service.build_dashboard()

    print("=" * 100)
    print("EDGE DASHBOARD v3")
    print("=" * 100)
    print(f"Runtime dir:              {result['runtime_dir']}")
    print(f"Output dir:               {result['output_dir']}")
    print(f"Snapshot path:            {result['snapshot_path']}")
    print(f"Journal path:             {result['journal_path']}")
    print(f"Snapshot rows:            {result['snapshot_rows']}")
    print(f"Journal rows:             {result['journal_rows']}")
    print(f"Summary JSON:             {result['summary_path']}")
    print(f"Scenario edge summary:    {result['scenario_edge_summary_path']}")
    print(f"Time edge summary:        {result['time_edge_summary_path']}")
    print("CSV files:")
    for csv_file in result["csv_files"]:
        print(f"  - {csv_file}")
    print("=" * 100)

    if args.print_summary:
        print(json.dumps(result["summary"], indent=2, ensure_ascii=False))

    if args.print_scenario_edge:
        print(json.dumps(result["scenario_edge_summary"], indent=2, ensure_ascii=False))

    if args.print_time_edge:
        print(json.dumps(result["time_edge_summary"], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()