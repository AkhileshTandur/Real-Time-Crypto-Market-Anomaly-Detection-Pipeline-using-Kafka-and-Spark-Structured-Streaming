"""Generate exploratory charts from cleaned Coinbase trade files."""

from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).parent.parent
CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned"
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_cleaned_data() -> pd.DataFrame:
    files = list(CLEANED_DIR.glob("coinbase_cleaned_trades_*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No cleaned data in {CLEANED_DIR}. Run processing/coinbase_data_cleaning.py first."
        )
    latest = max(files, key=lambda f: f.stat().st_mtime)
    df = pd.read_csv(latest)
    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(df["trade_time"], utc=True, errors="coerce")
    else:
        df["trade_time"] = pd.date_range(start="2024-01-01", periods=len(df), freq="s", tz="UTC")
    print(f"Loaded {len(df):,} records from {latest.name}")
    return df


def plot_price_over_time(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 5))
    for product in df["product_id"].unique():
        sub = df[df["product_id"] == product].copy()
        sub = sub.sort_values("trade_time")
        if len(sub) > 2000:
            sub = sub.iloc[:: max(len(sub) // 2000, 1)]
        ax.plot(sub["trade_time"], sub["price"], label=product, alpha=0.85)
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Price (USD)")
    ax.set_title("Coinbase Trade Price Over Time by Product")
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "coinbase_1_price_over_time.png", dpi=150)
    plt.close()
    print("Saved: coinbase_1_price_over_time.png")


def plot_volume_by_product(df: pd.DataFrame) -> None:
    vol = df.groupby("product_id")["quantity"].sum()
    fig, ax = plt.subplots(figsize=(8, 5))
    vol.plot(kind="bar", ax=ax, color=["#f7931a", "#627eea"])
    ax.set_xlabel("Product")
    ax.set_ylabel("Total Volume (base currency)")
    ax.set_title("Coinbase Total Trade Volume by Product")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "coinbase_2_volume_by_product.png", dpi=150)
    plt.close()
    print("Saved: coinbase_2_volume_by_product.png")


def plot_trade_count_by_hour(df: pd.DataFrame) -> None:
    df = df.copy()
    df["hour"] = df["trade_time"].dt.hour
    hourly = df.groupby("hour").size()
    fig, ax = plt.subplots(figsize=(10, 5))
    hourly.plot(kind="bar", ax=ax, color="steelblue", edgecolor="white")
    ax.set_xlabel("Hour of Day (UTC)")
    ax.set_ylabel("Number of Trades")
    ax.set_title("Coinbase Trade Count by Hour of Day")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "coinbase_3_trade_count_by_hour.png", dpi=150)
    plt.close()
    print("Saved: coinbase_3_trade_count_by_hour.png")


def plot_trade_value_distribution(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    df["trade_value"].hist(bins=50, ax=ax, color="steelblue", edgecolor="white")
    ax.set_xlabel("Trade Value (USD)")
    ax.set_ylabel("Frequency")
    ax.set_title("Coinbase Distribution of Trade Value")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "coinbase_4_trade_value_distribution.png", dpi=150)
    plt.close()
    print("Saved: coinbase_4_trade_value_distribution.png")


def plot_price_distribution_by_product(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    df.boxplot(column="price", by="product_id", ax=ax)
    ax.set_xlabel("Product")
    ax.set_ylabel("Price (USD)")
    ax.set_title("Coinbase Price Distribution by Product")
    plt.suptitle("")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "coinbase_5_price_distribution_by_product.png", dpi=150)
    plt.close()
    print("Saved: coinbase_5_price_distribution_by_product.png")


def plot_trade_count_by_product(df: pd.DataFrame) -> None:
    cnt = df.groupby("product_id").size()
    fig, ax = plt.subplots(figsize=(8, 5))
    cnt.plot(kind="bar", ax=ax, color=["#f7931a", "#627eea"])
    ax.set_xlabel("Product")
    ax.set_ylabel("Number of Trades")
    ax.set_title("Coinbase Trade Count by Product")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "coinbase_6_trade_count_by_product.png", dpi=150)
    plt.close()
    print("Saved: coinbase_6_trade_count_by_product.png")


def print_summary_stats(df: pd.DataFrame) -> None:
    print("\n--- COINBASE SUMMARY STATISTICS ---")
    print(f"Total records: {len(df):,}")
    print(f"Products:      {list(df['product_id'].unique())}")
    print(f"Time range:    {df['trade_time'].min()} to {df['trade_time'].max()}")
    print(f"\nPrice (USD):    mean={df['price'].mean():.2f}, median={df['price'].median():.2f}")
    print(f"Quantity:       mean={df['quantity'].mean():.4f}")
    print(f"Trade value:    mean={df['trade_value'].mean():.2f} USD")


def run_eda() -> None:
    df = load_cleaned_data()
    print_summary_stats(df)
    print("\nGenerating Coinbase EDA charts...")
    plot_price_over_time(df)
    plot_volume_by_product(df)
    plot_trade_count_by_hour(df)
    plot_trade_value_distribution(df)
    plot_price_distribution_by_product(df)
    plot_trade_count_by_product(df)
    print(f"\nAll charts saved to {OUTPUT_DIR}")


if __name__ == "__main__":
    run_eda()
