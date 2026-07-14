"""
Positioning Intelligence Layer v0.1.

Research-only context layer for AI Market Analyst.

This package does NOT generate trade signals and does NOT modify Battle Gate.
It only reads daily positioning proxy data, normalizes it, generates context tags,
stores snapshots/history, and renders a short briefing block.
"""

from .positioning_service import (
    POSITIONING_LAYER_VERSION,
    build_daily_positioning_context,
    get_latest_positioning_context,
)

__all__ = [
    "POSITIONING_LAYER_VERSION",
    "build_daily_positioning_context",
    "get_latest_positioning_context",
]
