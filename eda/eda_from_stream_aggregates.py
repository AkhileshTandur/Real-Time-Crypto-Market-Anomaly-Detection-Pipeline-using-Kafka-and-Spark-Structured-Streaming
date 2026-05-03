"""Build charts from Spark streaming aggregate CSV files."""

from __future__ import annotations

import argparse
import glob
from pathlib import Path

import pandas as pd
import matplotlib

matplotlib.use("Agg")
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

    df_time = df.groupby("window_start")["volume"].sum().reset_index()
    plt.figure(figsize=(12, 5))
    plt.plot(df_time["window_start"], df_time["volume"], color="#f7931a")
    plt.xlabel("Window Start Time")
    plt.ylabel("Total Volume (quantity)")
    plt.title("Total Trade Volume Over Time (Spark window aggregates)")
    plt.tight_layout()
    plt.savefig(out_dir / "stream_3_volume_over_time.png", dpi=150)
    plt.close()

    if "is_anomaly" in df.columns:
        df["is_anomaly"] = df["is_anomaly"].astype(str).str.lower().isin(["true", "1"])
        anomaly_counts = df.groupby("window_start")["is_anomaly"].sum().reset_index()
        plt.figure(figsize=(12, 5))
        plt.bar(anomaly_counts["window_start"], anomaly_counts["is_anomaly"], color="#c2410c")
        plt.xlabel("Window Start Time")
        plt.ylabel("Anomaly Windows")
        plt.title("Detected Market Anomaly Windows")
        plt.tight_layout()
        plt.savefig(out_dir / "stream_4_anomaly_windows.png", dpi=150)
        plt.close()

    print("EDA done. Saved:")
    print(f" - {out_dir / 'stream_1_trade_count_by_hour.png'}")
    print(f" - {out_dir / 'stream_2_avg_price_over_time.png'}")
    print(f" - {out_dir / 'stream_3_volume_over_time.png'}")
    if "is_anomaly" in df.columns:
        print(f" - {out_dir / 'stream_4_anomaly_windows.png'}")


if __name__ == "__main__":
    main()

