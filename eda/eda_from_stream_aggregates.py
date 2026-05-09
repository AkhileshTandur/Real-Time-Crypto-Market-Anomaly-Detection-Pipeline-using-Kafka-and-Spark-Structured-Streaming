"""Build charts from Spark streaming aggregate CSV files (Coinbase pipeline)."""

from __future__ import annotations

import argparse
import glob
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input_dir",
        default=str(Path("data") / "stream" / "coinbase_aggregates_csv"),
    )
    p.add_argument("--output_dir", default="output")
    p.add_argument("--product_ids", default="BTC-USD,ETH-USD")
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
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True, errors="coerce")

    products = [p.strip().upper() for p in args.product_ids.split(",") if p.strip()]
    df = df[df["product_id"].isin(products)]

    df["hour"] = df["window_start"].dt.hour
    hourly = df.groupby("hour")["trade_count"].sum().reset_index()

    plt.figure(figsize=(10, 5))
    plt.bar(hourly["hour"], hourly["trade_count"], color="steelblue", edgecolor="white")
    plt.xlabel("Hour of Day (UTC)")
    plt.ylabel("Total Trade Count")
    plt.title("Coinbase Trade Count Pattern (from Spark window aggregates)")
    plt.tight_layout()
    plt.savefig(out_dir / "coinbase_stream_1_trade_count_by_hour.png", dpi=150)
    plt.close()

    plt.figure(figsize=(12, 5))
    for product in products:
        sub = df[df["product_id"] == product].sort_values("window_start").copy()
        if len(sub) > 2000:
            sub = sub.iloc[:: max(len(sub) // 2000, 1)]
        plt.plot(sub["window_start"], sub["avg_price"], label=product, alpha=0.9)

    plt.xlabel("Window Start (UTC)")
    plt.ylabel("Average Price (USD)")
    plt.title("Coinbase Average Price Over Time (Spark window aggregates)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "coinbase_stream_2_avg_price_over_time.png", dpi=150)
    plt.close()

    df_time = df.groupby("window_start")["total_quantity"].sum().reset_index()
    plt.figure(figsize=(12, 5))
    plt.plot(df_time["window_start"], df_time["total_quantity"], color="#f7931a")
    plt.xlabel("Window Start (UTC)")
    plt.ylabel("Total Volume (base currency)")
    plt.title("Coinbase Total Trade Volume Over Time (Spark window aggregates)")
    plt.tight_layout()
    plt.savefig(out_dir / "coinbase_stream_3_volume_over_time.png", dpi=150)
    plt.close()

    if "is_anomaly" in df.columns:
        df["is_anomaly"] = df["is_anomaly"].astype(str).str.lower().isin(["true", "1"])
        anomaly_counts = df.groupby("window_start")["is_anomaly"].sum().reset_index()
        plt.figure(figsize=(12, 5))
        plt.bar(anomaly_counts["window_start"], anomaly_counts["is_anomaly"], color="#c2410c")
        plt.xlabel("Window Start (UTC)")
        plt.ylabel("Anomaly Windows")
        plt.title("Coinbase Detected Anomaly Windows")
        plt.tight_layout()
        plt.savefig(out_dir / "coinbase_stream_4_anomaly_windows.png", dpi=150)
        plt.close()

    print("Coinbase stream EDA done. Saved:")
    print(f" - {out_dir / 'coinbase_stream_1_trade_count_by_hour.png'}")
    print(f" - {out_dir / 'coinbase_stream_2_avg_price_over_time.png'}")
    print(f" - {out_dir / 'coinbase_stream_3_volume_over_time.png'}")
    if "is_anomaly" in df.columns:
        print(f" - {out_dir / 'coinbase_stream_4_anomaly_windows.png'}")


if __name__ == "__main__":
    main()
