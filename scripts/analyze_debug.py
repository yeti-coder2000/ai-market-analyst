from __future__ import annotations

from pathlib import Path

import pandas as pd


DEBUG_FILE = Path("app/debug/setup_a_debug.csv")


def print_separator(char: str = "=", n: int = 72) -> None:
    print(char * n)


def safe_pct(part: int, total: int) -> str:
    if total == 0:
        return "0.00%"
    return f"{(part / total) * 100:.2f}%"


def main() -> None:
    if not DEBUG_FILE.exists():
        print(f"Файл не знайдено: {DEBUG_FILE}")
        return

    df = pd.read_csv(DEBUG_FILE)

    if df.empty:
        print("CSV існує, але він порожній.")
        return

    print_separator()
    print("SETUP A DEBUG ANALYSIS")
    print_separator()

    total = len(df)
    print(f"Усього записів: {total}")

    print("\nСТАТУСИ SETUP A")
    status_counts = df["setup_a_status"].value_counts(dropna=False)
    for status, count in status_counts.items():
        print(f"  {status}: {count} ({safe_pct(count, total)})")

    print("\nHTF BIAS")
    bias_counts = df["htf_bias"].value_counts(dropna=False)
    for bias, count in bias_counts.items():
        print(f"  {bias}: {count} ({safe_pct(count, total)})")

    print("\nMARKET STATE")
    state_counts = df["market_state"].value_counts(dropna=False)
    for state, count in state_counts.items():
        print(f"  {state}: {count} ({safe_pct(count, total)})")

    # Numeric columns to inspect
    numeric_cols = [
        "impulse_atr_multiple",
        "impulse_body_ratio",
        "impulse_internal_pullback_pct",
        "impulse_range_points",
        "current_price",
    ]

    print("\nСЕРЕДНІ ЗНАЧЕННЯ ПО ВСІХ ЗАПИСАХ")
    for col in numeric_cols:
        if col in df.columns:
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            if not series.empty:
                print(
                    f"  {col}: "
                    f"mean={series.mean():.4f}, "
                    f"median={series.median():.4f}, "
                    f"min={series.min():.4f}, "
                    f"max={series.max():.4f}"
                )

    # Focus on WATCH
    if "setup_a_status" in df.columns:
        watch_df = df[df["setup_a_status"] == "WATCH"].copy()
        print("\nWATCH-СТАН")
        print(f"  Кількість: {len(watch_df)} ({safe_pct(len(watch_df), total)})")

        if not watch_df.empty:
            for col in ["impulse_atr_multiple", "impulse_body_ratio", "impulse_internal_pullback_pct"]:
                if col in watch_df.columns:
                    series = pd.to_numeric(watch_df[col], errors="coerce").dropna()
                    if not series.empty:
                        print(
                            f"  {col}: "
                            f"mean={series.mean():.4f}, "
                            f"median={series.median():.4f}, "
                            f"min={series.min():.4f}, "
                            f"max={series.max():.4f}"
                        )

    # Analyze failed checks
    print("\nНАЙЧАСТІШІ ПРИЧИНИ ВІДМОВИ ІМПУЛЬСУ")
    if "impulse_checks_failed" in df.columns:
        failed_series = df["impulse_checks_failed"].fillna("").astype(str)

        exploded = (
            failed_series
            .str.split(" | ", regex=False)
            .explode()
            .str.strip()
        )

        exploded = exploded[(exploded != "") & (exploded != "nan")]

        if not exploded.empty:
            fail_counts = exploded.value_counts()
            for reason, count in fail_counts.head(10).items():
                print(f"  {reason}: {count}")
        else:
            print("  Немає зафіксованих причин відмови.")

    # Ready subset
    ready_df = df[df["setup_a_status"] == "READY"].copy()
    print("\nREADY-СТАН")
    print(f"  Кількість: {len(ready_df)} ({safe_pct(len(ready_df), total)})")

    if not ready_df.empty:
        for col in ["impulse_atr_multiple", "impulse_body_ratio", "impulse_internal_pullback_pct"]:
            if col in ready_df.columns:
                series = pd.to_numeric(ready_df[col], errors="coerce").dropna()
                if not series.empty:
                    print(
                        f"  {col}: "
                        f"mean={series.mean():.4f}, "
                        f"median={series.median():.4f}, "
                        f"min={series.min():.4f}, "
                        f"max={series.max():.4f}"
                    )

    print("\nВИСНОВОК")
    if len(ready_df) == 0:
        print("  Поки що READY-сигналів немає. Потрібно накопичити більше спостережень.")
    else:
        print("  Уже є READY-сигнали — можна починати порівнювати їх профіль із WATCH.")

    print_separator()


if __name__ == "__main__":
    main()