"""
Data Preprocessing and Cleaning
Loads raw Binance trade data, cleans it, and saves for EDA.
Shows: null handling, invalid value filtering, type conversion, schema standardization.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned"
CLEANED_DIR.mkdir(parents=True, exist_ok=True)




    print(f"Loading: {raw_file}")
    records = []
    with open(raw_file) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    df = pd.DataFrame(records)
    print(f"Loaded {len(df):,} raw records")
    return df, raw_file


def clean_trades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and preprocess trade data.
    - Parse timestamps
    - Convert numeric types
    - Filter invalid values
    - Drop duplicates
    """
    initial_count = len(df)
    print("\n--- CLEANING STEPS ---")

    # 1. Rename Binance fields FIRST (before lowercasing - "T" and "t" are different)
    column_map = {"s": "symbol", "t": "trade_id", "p": "price", "q": "quantity", "T": "trade_time"}
    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
    df = df.loc[:, ~df.columns.duplicated()]

    # 2. Type conversion (only for columns that exist)
    for col in ["price", "quantity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "trade_id" in df.columns:
        df["trade_id"] = pd.to_numeric(df["trade_id"], errors="coerce")
    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(pd.to_numeric(df["trade_time"], errors="coerce"), unit="ms", errors="coerce")

    # 3. Drop rows with null in critical fields
    before_nulls = len(df)
    df = df.dropna(subset=["price", "quantity", "symbol"])
    print(f"1. Nulls removed: {before_nulls - len(df):,} rows")

    # 4. Filter invalid numeric values
    before_invalid = len(df)
    df = df[(df["price"] > 0) & (df["price"] < 1e9)]  # Reasonable price range
    df = df[(df["quantity"] > 0) & (df["quantity"] < 1e9)]  # Reasonable quantity
    print(f"2. Invalid price/quantity removed: {before_invalid - len(df):,} rows")

    # 5. Add derived column: trade value
    df["trade_value"] = df["price"] * df["quantity"]

    # 6. Drop duplicates by trade_id
    before_dup = len(df)
    df = df.drop_duplicates(subset=["trade_id"], keep="first")
    print(f"3. Duplicates removed: {before_dup - len(df):,} rows")

    # 7. Sort by time
    if "trade_time" in df.columns:
        df = df.sort_values("trade_time").reset_index(drop=True)

    final_count = len(df)
    print(f"\nBefore cleaning: {initial_count:,} | After: {final_count:,} | Removed: {initial_count - final_count:,}")
    return df


def save_cleaned(df: pd.DataFrame, raw_file: Path) -> Path:
    """Save cleaned data to CSV."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = CLEANED_DIR / f"cleaned_trades_{timestamp}.csv"
    df.to_csv(out_file, index=False)
    print(f"Saved to {out_file}")
    return out_file


def run_cleaning(raw_file: Path = None):
    """Full pipeline: load, clean, save."""
    df, used_file = load_raw_data(raw_file)
    df_clean = clean_trades(df)
    out = save_cleaned(df_clean, used_file)
    return df_clean, out


if __name__ == "__main__":
    df_clean, _ = run_cleaning()
    print("\nSample of cleaned data:")
    print(df_clean.head(10))
