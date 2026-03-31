from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from app.context.schema import MarketContext, SetupAResult


class DebugLogger:
    def __init__(self, filepath: str = "app/debug/setup_a_debug.csv") -> None:
        self.filepath = Path(filepath)
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

        self.headers = [
            "timestamp",
            "instrument",
            "current_price",
            "market_state",
            "htf_bias",
            "weekly_val",
            "weekly_poc",
            "weekly_vah",
            "daily_val",
            "daily_poc",
            "daily_vah",
            "accepted_above_weekly_vah",
            "accepted_below_weekly_val",
            "no_acceptance_above",
            "no_acceptance_below",
            "structure_4h_bos_up",
            "structure_4h_bos_down",
            "structure_4h_hh_hl",
            "structure_4h_ll_lh",
            "structure_15m_bos_up",
            "structure_15m_bos_down",
            "structure_15m_hh_hl",
            "structure_15m_ll_lh",
            "impulse_detected",
            "impulse_direction",
            "impulse_range_points",
            "impulse_atr_multiple",
            "impulse_body_ratio",
            "impulse_internal_pullback_pct",
            "impulse_broke_local_balance",
            "impulse_min_atr_required",
            "impulse_min_body_required",
            "impulse_max_pullback_allowed",
            "impulse_checks_passed",
            "impulse_checks_failed",
            "pullback_detected",
            "pullback_direction",
            "pullback_depth_pct",
            "pullback_held_structure",
            "setup_a_status",
            "setup_a_direction",
            "setup_a_grade",
            "setup_a_has_entry_plan",
            "setup_a_failed_conditions",
            "setup_a_passed_conditions",
        ]

        if not self.filepath.exists():
            with self.filepath.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writeheader()

    @staticmethod
    def _join_items(items: list[Any]) -> str:
        return " | ".join(str(x) for x in items) if items else ""

    def log_setup_a(self, context: MarketContext, result: SetupAResult) -> None:
        impulse_debug = context.impulse.debug

        row = {
            "timestamp": context.timestamp.isoformat(),
            "instrument": context.instrument.value,
            "current_price": context.current_price,
            "market_state": context.market_state.value,
            "htf_bias": context.htf_bias.bias.value,
            "weekly_val": context.profile.weekly.val,
            "weekly_poc": context.profile.weekly.poc,
            "weekly_vah": context.profile.weekly.vah,
            "daily_val": context.profile.daily.val,
            "daily_poc": context.profile.daily.poc,
            "daily_vah": context.profile.daily.vah,
            "accepted_above_weekly_vah": context.acceptance.accepted_above,
            "accepted_below_weekly_val": context.acceptance.accepted_below,
            "no_acceptance_above": context.acceptance.no_acceptance_above,
            "no_acceptance_below": context.acceptance.no_acceptance_below,
            "structure_4h_bos_up": context.structure_4h.bos_up,
            "structure_4h_bos_down": context.structure_4h.bos_down,
            "structure_4h_hh_hl": context.structure_4h.hh_hl_structure,
            "structure_4h_ll_lh": context.structure_4h.ll_lh_structure,
            "structure_15m_bos_up": context.structure_15m.bos_up,
            "structure_15m_bos_down": context.structure_15m.bos_down,
            "structure_15m_hh_hl": context.structure_15m.hh_hl_structure,
            "structure_15m_ll_lh": context.structure_15m.ll_lh_structure,
            "impulse_detected": context.impulse.detected,
            "impulse_direction": context.impulse.direction.value,
            "impulse_range_points": context.impulse.range_points,
            "impulse_atr_multiple": context.impulse.range_atr_multiple,
            "impulse_body_ratio": context.impulse.body_ratio,
            "impulse_internal_pullback_pct": context.impulse.internal_pullback_pct,
            "impulse_broke_local_balance": context.impulse.broke_local_balance,
            "impulse_min_atr_required": impulse_debug.min_atr_multiple_required if impulse_debug else None,
            "impulse_min_body_required": impulse_debug.min_body_ratio_required if impulse_debug else None,
            "impulse_max_pullback_allowed": impulse_debug.max_internal_pullback_allowed if impulse_debug else None,
            "impulse_checks_passed": self._join_items(impulse_debug.checks_passed if impulse_debug else []),
            "impulse_checks_failed": self._join_items(impulse_debug.checks_failed if impulse_debug else []),
            "pullback_detected": context.pullback.detected,
            "pullback_direction": context.pullback.direction.value,
            "pullback_depth_pct": context.pullback.depth_pct_of_impulse,
            "pullback_held_structure": context.pullback.held_structure,
            "setup_a_status": result.status.value,
            "setup_a_direction": result.direction.value,
            "setup_a_grade": result.grade.value if result.grade else "",
            "setup_a_has_entry_plan": result.entry_plan is not None,
            "setup_a_failed_conditions": self._join_items(
                [f"{c.name}:{c.message}" for c in result.diagnostics.failed_conditions]
            ),
            "setup_a_passed_conditions": self._join_items(
                [f"{c.name}:{c.message}" for c in result.diagnostics.passed_conditions]
            ),
        }

        with self.filepath.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerow(row)