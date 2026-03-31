from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from schema import SetupStatus, Direction


@dataclass
class RadarRow:
    instrument: str
    setup: str
    status: str
    direction: str
    grade: str | None
    score: float
    summary: List[str]
    trigger_price: Optional[float] = None
    distance_points: Optional[float] = None
    distance_atr: Optional[float] = None
    distance_comment: str = ""


class MarketRadar:

    def __init__(self):
        self.rows: List[RadarRow] = []

    def _score_status(self, status: SetupStatus) -> int:
        ranking = {
            SetupStatus.TRIGGERED: 10,
            SetupStatus.READY: 9,
            SetupStatus.PULLBACK_IN_PROGRESS: 7,
            SetupStatus.RETURNING_TO_VALUE: 6,
            SetupStatus.IMPULSE_FOUND: 5,
            SetupStatus.SWEEP_DETECTED: 5,
            SetupStatus.WATCH: 3,
            SetupStatus.IDLE: 1,
            SetupStatus.INVALIDATED: 0,
        }
        return ranking.get(status, 0)

    def _score_grade(self, grade: str | None) -> int:
        if grade == "A":
            return 3
        if grade == "B":
            return 2
        if grade == "C":
            return 1
        return 0

    def _score_direction(self, direction: Direction, bias: Direction) -> int:
        if direction == bias and direction != Direction.NEUTRAL:
            return 2
        return 0

    def _distance_bonus(self, distance_atr: float | None) -> float:
        if distance_atr is None:
            return 0.0
        if distance_atr <= 0.25:
            return 3.0
        if distance_atr <= 0.50:
            return 2.0
        if distance_atr <= 1.00:
            return 1.0
        return 0.0

    def calculate_score(self, context, result, distance_info=None) -> float:
        status_score = self._score_status(result.status)
        grade_score = self._score_grade(result.grade.value if result.grade else None)
        direction_score = self._score_direction(result.direction, context.htf_bias.bias)

        distance_atr = getattr(distance_info, "distance_atr", None)
        distance_bonus = self._distance_bonus(distance_atr)

        return status_score + grade_score + direction_score + distance_bonus

    def add(self, context, result, distance_info=None):
        total_score = self.calculate_score(context, result, distance_info)

        self.rows.append(
            RadarRow(
                instrument=context.instrument.value,
                setup=result.setup_type.value,
                status=result.status.value,
                direction=result.direction.value,
                grade=result.grade.value if result.grade else None,
                score=total_score,
                summary=result.context_summary or [],
                trigger_price=getattr(distance_info, "trigger_price", None),
                distance_points=getattr(distance_info, "distance_points", None),
                distance_atr=getattr(distance_info, "distance_atr", None),
                distance_comment=getattr(distance_info, "comment", ""),
            )
        )

    def top(self, n: int = 3) -> List[RadarRow]:
        return sorted(self.rows, key=lambda r: r.score, reverse=True)[:n]

    @staticmethod
    def _fmt(v):
        if v is None:
            return "-"
        try:
            return f"{float(v):.2f}"
        except Exception:
            return str(v)

    def print(self, n: int = 3):
        print("\n================ MARKET RADAR ================")

        top_rows = self.top(n)

        if not top_rows:
            print("Немає активних кандидатів.")
            return

        for r in top_rows:
            print(f"\n{r.instrument}")
            print(f"  Setup:      {r.setup}")
            print(f"  Status:     {r.status}")
            print(f"  Direction:  {r.direction}")
            print(f"  Grade:      {r.grade or '-'}")
            print(f"  Score:      {r.score:.2f}")
            print(f"  Trigger:    {self._fmt(r.trigger_price)}")
            print(f"  Dist pts:   {self._fmt(r.distance_points)}")
            print(f"  Dist ATR:   {self._fmt(r.distance_atr)}")
            print(f"  Comment:    {r.distance_comment or '-'}")

            if r.summary:
                print("  Context:")
                for s in r.summary[:3]:
                    print(f"   - {s}")