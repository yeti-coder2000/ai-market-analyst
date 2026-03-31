from __future__ import annotations

from pathlib import Path

import pandas as pd


RADAR_LOG = Path("app/signals/market_radar_log.csv")


def sep(char: str = "=", n: int = 72) -> None:
    print(char * n)


def safe_pct(part: int, total: int) -> str:
    if total == 0:
        return "0.00%"
    return f"{(part / total) * 100:.2f}%"


def print_group_stats(df: pd.DataFrame, group_col: str, title: str) -> None:
    print(f"\n{title}")
    counts = df[group_col].fillna("NA").value_counts(dropna=False)

    total = len(df)
    for key, count in counts.items():
        print(f"  {key}: {count} ({safe_pct(count, total)})")


def print_numeric_summary(df: pd.DataFrame, cols: list[str], title: str) -> None:
    print(f"\n{title}")
    for col in cols:
        if col not in df.columns:
            continue

        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            print(f"  {col}: немає даних")
            continue

        print(
            f"  {col}: "
            f"mean={series.mean():.4f}, "
            f"median={series.median():.4f}, "
            f"min={series.min():.4f}, "
            f"max={series.max():.4f}"
        )


def print_instrument_score_table(df: pd.DataFrame) -> None:
    print("\nСЕРЕДНІ ПО ІНСТРУМЕНТАХ")

    tmp = df.copy()

    tmp["radar_score"] = pd.to_numeric(tmp["radar_score"], errors="coerce")
    tmp["distance_atr"] = pd.to_numeric(tmp["distance_atr"], errors="coerce")

    grouped = (
        tmp.groupby("instrument", dropna=False)
        .agg(
            signals=("instrument", "count"),
            avg_score=("radar_score", "mean"),
            best_score=("radar_score", "max"),
            avg_distance_atr=("distance_atr", "mean"),
        )
        .sort_values(["avg_score", "signals"], ascending=[False, False])
    )

    if grouped.empty:
        print("  Немає даних.")
        return

    for instrument, row in grouped.iterrows():
        avg_score = row["avg_score"]
        best_score = row["best_score"]
        avg_distance = row["avg_distance_atr"]

        avg_score_str = "-" if pd.isna(avg_score) else f"{avg_score:.2f}"
        best_score_str = "-" if pd.isna(best_score) else f"{best_score:.2f}"
        avg_distance_str = "-" if pd.isna(avg_distance) else f"{avg_distance:.2f}"

        print(
            f"  {instrument}: "
            f"signals={int(row['signals'])}, "
            f"avg_score={avg_score_str}, "
            f"best_score={best_score_str}, "
            f"avg_distance_atr={avg_distance_str}"
        )


def print_ready_watch_focus(df: pd.DataFrame) -> None:
    print("\nФОКУС НА WATCH / READY")

    if "status" not in df.columns:
        print("  Колонка status не знайдена.")
        return

    subset = df[df["status"].isin(["WATCH", "READY"])].copy()

    if subset.empty:
        print("  Немає WATCH/READY записів.")
        return

    print(f"  Усього WATCH/READY записів: {len(subset)}")

    counts = subset["status"].value_counts()
    for status, count in counts.items():
        print(f"  {status}: {count}")

    print_numeric_summary(
        subset,
        cols=["radar_score", "distance_atr", "distance_points"],
        title="  Метрики для WATCH/READY",
    )


def print_top_candidates(df: pd.DataFrame, top_n: int = 10) -> None:
    print(f"\nTOP-{top_n} ІСТОРИЧНИХ КАНДИДАТІВ")

    tmp = df.copy()
    tmp["radar_score"] = pd.to_numeric(tmp["radar_score"], errors="coerce")
    tmp["distance_atr"] = pd.to_numeric(tmp["distance_atr"], errors="coerce")

    tmp = tmp.sort_values(
        by=["radar_score", "distance_atr"],
        ascending=[False, True],
        na_position="last",
    )

    top = tmp.head(top_n)

    if top.empty:
        print("  Немає даних.")
        return

    for _, row in top.iterrows():
        print(
            f"  {row.get('timestamp', '-')}"
            f" | {row.get('instrument', '-')}"
            f" | {row.get('setup', '-')}"
            f" | {row.get('status', '-')}"
            f" | score={row.get('radar_score', '-')}"
            f" | dist_atr={row.get('distance_atr', '-')}"
        )


def main() -> None:
    if not RADAR_LOG.exists():
        print(f"Файл не знайдено: {RADAR_LOG}")
        return

    df = pd.read_csv(RADAR_LOG)

    if df.empty:
        print("Журнал радара існує, але порожній.")
        return

    sep()
    print("RADAR JOURNAL ANALYSIS")
    sep()

    total = len(df)
    print(f"Усього записів: {total}")

    print_group_stats(df, "instrument", "ІНСТРУМЕНТИ")
    print_group_stats(df, "setup", "СЕТАПИ")
    print_group_stats(df, "status", "СТАТУСИ")
    print_group_stats(df, "market_state", "MARKET STATE")
    print_group_stats(df, "htf_bias", "HTF BIAS")

    print_numeric_summary(
        df,
        cols=["radar_score", "distance_atr", "distance_points"],
        title="ЗАГАЛЬНІ СЕРЕДНІ",
    )

    print_instrument_score_table(df)
    print_ready_watch_focus(df)
    print_top_candidates(df, top_n=10)

    sep()


if __name__ == "__main__":
    main()