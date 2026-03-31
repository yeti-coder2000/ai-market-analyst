"""
Scenario Summary Builder

Transforms ScenarioResult into human-readable output.

Used by:
- console renderer
- market radar
- journaling
"""

from typing import List

from app.core.types import ScenarioResult


class SummaryBuilder:
    """
    Builds structured summary for logs and reports.
    """

    def build_summary(self, scenario: ScenarioResult) -> List[str]:
        """
        Convert scenario into formatted lines.
        """

        lines: List[str] = []

        lines.append(f"Instrument:   {scenario.instrument}")
        lines.append(f"Setup:        {scenario.setup_name}")
        lines.append(f"Status:       {scenario.status}")
        lines.append(f"Direction:    {scenario.direction}")
        lines.append(f"Score:        {scenario.score:.2f}")

        if scenario.entry_trigger:
            lines.append(f"Trigger:      {scenario.entry_trigger}")

        lines.append(f"HTF bias:     {scenario.context_bias}")
        lines.append(f"Market state: {scenario.market_state}")

        if scenario.distance_points:
            lines.append(f"Dist pts:     {scenario.distance_points:.2f}")

        if scenario.distance_atr:
            lines.append(f"Dist ATR:     {scenario.distance_atr:.2f}")

        if scenario.comment:
            lines.append(f"Comment:      {scenario.comment}")

        if scenario.context_notes:
            lines.append("Context:")
            for note in scenario.context_notes:
                lines.append(f"  - {note}")

        return lines