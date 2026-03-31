from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


class RadarJournal:
    def __init__(self, filepath: str = "app/signals/market_radar_log.csv") -> None:
        self.filepath = Path(filepath)
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

        self.headers = [
            "timestamp",
            "instrument",
            "setup",
            "status",
            "direction",
            "grade",
            "htf_bias",
            "market_state",
            "trigger_price",
            "distance_points",
            "distance_atr",
            "radar_score",
            "summary",
        ]

        if not self.filepath.exists():
            with self.filepath.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writeheader()

    @staticmethod
    def _join_summary(items: list[Any]) -> str:
        return " | ".join(str(x) for x in items) if items else ""

    def log_signal(
        self,
        signal,
        distance_info,
        radar_score: float,
    ) -> None:
        row = {
            "timestamp": signal.timestamp.isoformat(),
            "instrument": signal.instrument.value,
            "setup": signal.setup_type.value,
            "status": signal.status.value,
            "direction": signal.direction.value,
            "grade": signal.grade.value if signal.grade else "",
            "htf_bias": signal.htf_bias.value,
            "market_state": signal.market_state.value,
            "trigger_price": distance_info.trigger_price,
            "distance_points": distance_info.distance_points,
            "distance_atr": distance_info.distance_atr,
            "radar_score": radar_score,
            "summary": self._join_summary(signal.context_summary),
        }

        with self.filepath.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerow(row)