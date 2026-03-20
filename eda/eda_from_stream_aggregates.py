"""
EDA plots from Spark streaming window aggregates CSV files.

This is meant for the mid-presentation:
- show cleaning effects (you can compute stats from the streaming output if needed)
- show demand/volume patterns across time windows

It reads all CSV files under --input_dir (as written by Spark).
"""

from __future__ import annotations

import argparse
import glob
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input_dir", default=str(Path("data") / "stream" / "aggregates_csv"))
    p.add_argument("--output_dir", default="output")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    input_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(glob.glob(str(input_dir / "**" / "*.csv"), recursive=True))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under: {input_dir}")

    df = pd.concat((pd.read_csv(fp) for fp in csv_files), ignore_index=True)
    df["window_start"] = pd.to_datetime(df["window_start"])

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    df = df[df["symbol"].isin(symbols)]

    # Chart 1: trade_count by hour (aggregated across symbols)
    df["hour"] = df["window_start"].dt.hour
    hourly = df.groupby("hour")["trade_count"].sum().reset_index()

    plt.figure(figsize=(10, 5))
    plt.bar(hourly["hour"], hourly["trade_count"], color="steelblue", edgecolor="white")
    plt.xlabel("Hour of Day (0-23)")
    plt.ylabel("Total Trade Count")
    plt.title("Trade Count Pattern (from Spark window aggregates)")
    plt.tight_layout()
    plt.savefig(out_dir / "stream_1_trade_count_by_hour.png", dpi=150)
    plt.close()

    # Chart 2: avg_price over time (sampled points for readability)
    plt.figure(figsize=(12, 5))
    for sym in symbols:
        sub = df[df["symbol"] == sym].sort_values("window_start").copy()
        if len(sub) > 2000:
            sub = sub.iloc[:: max(len(sub) // 2000, 1)]
        plt.plot(sub["window_start"], sub["avg_price"], label=sym, alpha=0.9)

    plt.xlabel("Window Start Time")
    plt.ylabel("Average Price")
    plt.title("Average Price Over Time (Spark window aggregates)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "stream_2_avg_price_over_time.png", dpi=150)
    plt.close()

    # Chart 3: volume over time (sum volume across symbols)
    df_time = df.groupby("window_start")["volume"].sum().reset_index()
    plt.figure(figsize=(12, 5))
    plt.plot(df_time["window_start"], df_time["volume"], color="#f7931a")
    plt.xlabel("Window Start Time")
    plt.ylabel("Total Volume (quantity)")
    plt.title("Total Trade Volume Over Time (Spark window aggregates)")
    plt.tight_layout()
    plt.savefig(out_dir / "stream_3_volume_over_time.png", dpi=150)
    plt.close()

    print("EDA done. Saved:")
    print(f" - {out_dir / 'stream_1_trade_count_by_hour.png'}")
    print(f" - {out_dir / 'stream_2_avg_price_over_time.png'}")
    print(f" - {out_dir / 'stream_3_volume_over_time.png'}")


if __name__ == "__main__":
    main()

