"""Offline rule-based anomaly scoring for cleaned Coinbase trade files.

Reads the latest ``data/cleaned/coinbase_cleaned_trades_*.csv`` produced by
``processing/coinbase_data_cleaning.py`` and computes per-product rolling
z-scores of price and trade_value. Records exceeding ``z_threshold`` (default
3.0) on either signal are flagged as anomalies.

Output:
    data/anomalies/latest_coinbase_trade_anomaly_scores.csv
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).parent.parent
CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned"
ANOMALY_DIR = PROJECT_ROOT / "data" / "anomalies"
ANOMALY_DIR.mkdir(parents=True, exist_ok=True)


def load_latest_cleaned() -> pd.DataFrame:
    files = list(CLEANED_DIR.glob("coinbase_cleaned_trades_*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No cleaned data found in {CLEANED_DIR}. "
            "Run processing/coinbase_data_cleaning.py first."
        )
    latest = max(files, key=lambda f: f.stat().st_mtime)
    df = pd.read_csv(latest)
    df["trade_time"] = pd.to_datetime(df["trade_time"], utc=True, errors="coerce")
    print(f"Loaded {len(df):,} cleaned trades from {latest.name}")
    return df


def score_anomalies(df: pd.DataFrame, window: int = 100, z_threshold: float = 3.0) -> pd.DataFrame:
    """Compute per-product rolling z-scores of price and trade_value.

    Per-product rolling baselines keep the score local to each product, so
    BTC-USD and ETH-USD are not compared on the same absolute price scale.
    """
    required = {"product_id", "trade_time", "price", "quantity", "trade_value"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df = df.sort_values(["product_id", "trade_time"]).copy()
    groups = df.groupby("product_id", group_keys=False)

    price_mean = (
        groups["price"].rolling(window=window, min_periods=20).mean().reset_index(level=0, drop=True)
    )
    price_std = (
        groups["price"].rolling(window=window, min_periods=20).std().reset_index(level=0, drop=True)
    )
    value_mean = (
        groups["trade_value"].rolling(window=window, min_periods=20).mean().reset_index(level=0, drop=True)
    )
    value_std = (
        groups["trade_value"].rolling(window=window, min_periods=20).std().reset_index(level=0, drop=True)
    )

    df["price_zscore"] = (
        ((df["price"] - price_mean) / price_std).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    )
    df["trade_value_zscore"] = (
        ((df["trade_value"] - value_mean) / value_std).replace([np.inf, -np.inf], 0.0).fillna(0.0)
    )
    df["is_anomaly"] = (df["price_zscore"].abs() >= z_threshold) | (
        df["trade_value_zscore"].abs() >= z_threshold
    )

    return df


def main() -> None:
    df = load_latest_cleaned()
    scored = score_anomalies(df)

    out_file = ANOMALY_DIR / "latest_coinbase_trade_anomaly_scores.csv"
    scored.to_csv(out_file, index=False)

    anomalies = scored[scored["is_anomaly"]]
    print(f"Scored trades: {len(scored):,}")
    print(f"Anomalies flagged: {len(anomalies):,}")
    print(f"Saved anomaly scores to {out_file}")

    if not anomalies.empty:
        print("\nTop anomalies by absolute z-score:")
        cols = [
            "trade_time",
            "product_id",
            "price",
            "quantity",
            "trade_value",
            "price_zscore",
            "trade_value_zscore",
        ]
        ranked = anomalies.assign(
            max_abs_score=anomalies[["price_zscore", "trade_value_zscore"]].abs().max(axis=1)
        )
        print(
            ranked.sort_values("max_abs_score", ascending=False)[cols]
            .head(10)
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()
