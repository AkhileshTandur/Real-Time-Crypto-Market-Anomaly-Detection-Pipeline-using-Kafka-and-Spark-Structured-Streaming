"""
Exploratory Data Analysis (EDA)
Creates visualizations for the mid-presentation:
- Price over time
- Volume by symbol
- Trade count by hour
- Trade value distribution
- Price distribution by symbol
"""

import pandas as pd
from pathlib import Path
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned"
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_cleaned_data():
    """Load most recent cleaned CSV."""
    files = list(CLEANED_DIR.glob("cleaned_trades_*.csv"))
    if not files:
        raise FileNotFoundError(f"No cleaned data in {CLEANED_DIR}. Run data_cleaning.py first.")
    latest = max(files, key=lambda f: f.stat().st_mtime)
    df = pd.read_csv(latest)
    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(df["trade_time"])
    else:
        # If trade_time is missing, create a monotonic time axis so plots still work.
        df["trade_time"] = pd.date_range(start="2024-01-01", periods=len(df), freq="s")
    print(f"Loaded {len(df):,} records from {latest.name}")
    return df


def plot_price_over_time(df: pd.DataFrame):
    """Price over time by symbol (sampled for readability)."""
    fig, ax = plt.subplots(figsize=(12, 5))
    for symbol in df["symbol"].unique():
        sub = df[df["symbol"] == symbol].copy()
        sub = sub.sort_values("trade_time")
        # Sample if too many points
        if len(sub) > 2000:
            sub = sub.iloc[:: len(sub) // 2000]
        ax.plot(sub["trade_time"], sub["price"], label=symbol, alpha=0.8)
    ax.set_xlabel("Time")
    ax.set_ylabel("Price (USDT)")
    ax.set_title("Trade Price Over Time by Symbol")
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "1_price_over_time.png", dpi=150)
    plt.close()
    print("Saved: 1_price_over_time.png")


def plot_volume_by_symbol(df: pd.DataFrame):
    """Total volume (quantity) by symbol."""
    vol = df.groupby("symbol")["quantity"].sum()
    fig, ax = plt.subplots(figsize=(8, 5))
    vol.plot(kind="bar", ax=ax, color=["#f7931a", "#627eea"])
    ax.set_xlabel("Symbol")
    ax.set_ylabel("Total Volume")
    ax.set_title("Total Trade Volume by Symbol")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "2_volume_by_symbol.png", dpi=150)
    plt.close()
    print("Saved: 2_volume_by_symbol.png")


def plot_trade_count_by_hour(df: pd.DataFrame):
    """Trade count by hour of day."""
    df = df.copy()
    df["hour"] = df["trade_time"].dt.hour
    hourly = df.groupby("hour").size()
    fig, ax = plt.subplots(figsize=(10, 5))
    hourly.plot(kind="bar", ax=ax, color="steelblue", edgecolor="white")
    ax.set_xlabel("Hour of Day (0-23)")
    ax.set_ylabel("Number of Trades")
    ax.set_title("Trade Count by Hour of Day")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "3_trade_count_by_hour.png", dpi=150)
    plt.close()
    print("Saved: 3_trade_count_by_hour.png")


def plot_trade_value_distribution(df: pd.DataFrame):
    """Distribution of trade value (price * quantity)."""
    fig, ax = plt.subplots(figsize=(10, 5))
    df["trade_value"].hist(bins=50, ax=ax, color="steelblue", edgecolor="white")
    ax.set_xlabel("Trade Value (USDT)")
    ax.set_ylabel("Frequency")
    ax.set_title("Distribution of Trade Value")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "4_trade_value_distribution.png", dpi=150)
    plt.close()
    print("Saved: 4_trade_value_distribution.png")


def plot_price_distribution_by_symbol(df: pd.DataFrame):
    """Price distribution by symbol (box plot)."""
    fig, ax = plt.subplots(figsize=(8, 5))
    df.boxplot(column="price", by="symbol", ax=ax)
    ax.set_xlabel("Symbol")
    ax.set_ylabel("Price (USDT)")
    ax.set_title("Price Distribution by Symbol")
    plt.suptitle("")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "5_price_distribution_by_symbol.png", dpi=150)
    plt.close()
    print("Saved: 5_price_distribution_by_symbol.png")


def plot_trade_count_by_symbol(df: pd.DataFrame):
    """Trade count by symbol."""
    cnt = df.groupby("symbol").size()
    fig, ax = plt.subplots(figsize=(8, 5))
    cnt.plot(kind="bar", ax=ax, color=["#f7931a", "#627eea"])
    ax.set_xlabel("Symbol")
    ax.set_ylabel("Number of Trades")
    ax.set_title("Trade Count by Symbol")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "6_trade_count_by_symbol.png", dpi=150)
    plt.close()
    print("Saved: 6_trade_count_by_symbol.png")


def print_summary_stats(df: pd.DataFrame):
    """Print summary statistics for presentation."""
    print("\n--- SUMMARY STATISTICS ---")
    print(f"Total records: {len(df):,}")
    print(f"Symbols: {list(df['symbol'].unique())}")
    print(f"Time range: {df['trade_time'].min()} to {df['trade_time'].max()}")
    print(f"\nPrice: mean={df['price'].mean():.2f}, median={df['price'].median():.2f}")
    print(f"Quantity: mean={df['quantity'].mean():.4f}")
    print(f"Trade value: mean={df['trade_value'].mean():.2f} USDT")


def run_eda():
    """Run all EDA and save charts."""
    df = load_cleaned_data()
    print_summary_stats(df)
    print("\nGenerating charts...")
    plot_price_over_time(df)
    plot_volume_by_symbol(df)
    plot_trade_count_by_hour(df)
    plot_trade_value_distribution(df)
    plot_price_distribution_by_symbol(df)
    plot_trade_count_by_symbol(df)
    print(f"\nAll charts saved to {OUTPUT_DIR}")


if __name__ == "__main__":
    run_eda()
