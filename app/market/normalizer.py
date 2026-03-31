from __future__ import annotations

import pandas as pd

from schema import FetchRequest


def normalize_twelvedata_payload(payload: dict, req: FetchRequest) -> pd.DataFrame:
    """
    TwelveData returns:
    {
      "meta": {...},
      "values": [
        {
          "datetime": "2026-01-20 10:15:00",
          "open": "2720.1",
          "high": "2721.4",
          ...
        }
      ]
    }
    """
    values = payload.get("values", [])
    if not values:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.DataFrame(values)

    rename_map = {"datetime": "timestamp"}
    df = df.rename(columns=rename_map)

    required = ["timestamp", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Normalized payload missing columns: {missing}")

    if "volume" not in df.columns:
        df["volume"] = 0.0

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = (
        df[["timestamp", "open", "high", "low", "close", "volume"]]
        .dropna(subset=["timestamp", "open", "high", "low", "close"])
        .sort_values("timestamp")
        .drop_duplicates(subset=["timestamp"], keep="last")
        .set_index("timestamp")
    )

    return df