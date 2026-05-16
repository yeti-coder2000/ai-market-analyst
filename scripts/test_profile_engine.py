from __future__ import annotations

import pandas as pd

from app.auction.profile_engine import (
    build_auction_context,
    auction_context_to_signal_filters,
)


def make_test_data() -> pd.DataFrame:
    rows = []

    def add_bar(ts, o, h, l, c, v=100):
        rows.append(
            {
                "timestamp": ts,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
            }
        )

    # Session 1
    add_bar("2026-05-13 09:30:00+00:00", 100, 103, 99, 102, 120)
    add_bar("2026-05-13 09:45:00+00:00", 102, 104, 101, 103, 180)
    add_bar("2026-05-13 10:00:00+00:00", 103, 105, 102, 104, 200)
    add_bar("2026-05-13 10:15:00+00:00", 104, 106, 103, 105, 160)

    # Session 2
    add_bar("2026-05-14 09:30:00+00:00", 104, 107, 103, 106, 150)
    add_bar("2026-05-14 09:45:00+00:00", 106, 108, 105, 107, 190)
    add_bar("2026-05-14 10:00:00+00:00", 107, 109, 106, 108, 210)
    add_bar("2026-05-14 10:15:00+00:00", 108, 110, 107, 109, 170)

    # Current session — opens out of previous range
    add_bar("2026-05-15 09:30:00+00:00", 112, 114, 111, 113, 200)
    add_bar("2026-05-15 09:45:00+00:00", 113, 115, 112, 114, 220)
    add_bar("2026-05-15 10:00:00+00:00", 114, 116, 113, 115, 180)
    add_bar("2026-05-15 10:15:00+00:00", 115, 117, 114, 116, 160)

    return pd.DataFrame(rows)


def main() -> None:
    df = make_test_data()

    context = build_auction_context(
        df,
        symbol="TEST",
        timeframe="15m",
        tick_size=1.0,
        value_area_pct=0.70,
        ib_minutes=60,
    )

    filters = auction_context_to_signal_filters(context)

    print("\n=== AUCTION CONTEXT ===")
    for k, v in context.to_dict().items():
        print(f"{k}: {v}")

    print("\n=== AUCTION FILTERS ===")
    for k, v in filters.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()