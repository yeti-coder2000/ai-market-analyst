from __future__ import annotations

"""
Production entrypoint for AI Market Analyst.

Keeping this module as a thin wrapper lets local operators run `python main.py`
while Render and process supervisors can continue to use module entrypoints.
"""

import sys

from app.runners.main_worker import main as run_main_worker


def main() -> int:
    """Run the production market-analysis worker."""
    return int(run_main_worker() or 0)


if __name__ == "__main__":
    sys.exit(main())
