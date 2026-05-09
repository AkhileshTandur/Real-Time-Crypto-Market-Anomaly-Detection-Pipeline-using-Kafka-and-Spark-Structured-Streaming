"""Offline cleaning for normalized Coinbase trade JSONL files.

Reads any ``coinbase_*.jsonl`` (live or synthetic) under ``data/raw/`` and
produces a cleaned CSV that the rule-based scorer
(``processing/anomaly_detection.py``) and the ML scorer
(``processing/ml_anomaly_detection.py``) consume.

Schema of the output CSV (one row per trade):
    source, product_id, trade_id, price, quantity, trade_time,
    side, raw_type, trade_value
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CLEANED_DIR = PROJECT_ROOT / "data" / "cleaned"
CLEANED_DIR.mkdir(parents=True, exist_ok=True)


def load_raw_data(raw_file: Path | None = None) -> tuple[pd.DataFrame, Path]:
    """Load every line of one or more Coinbase JSONL files into a DataFrame.

    If ``raw_file`` is None, the most recently modified ``coinbase_*.jsonl``
    file in ``data/raw/`` is used.
    """
    if raw_file is None:
        files = sorted(RAW_DIR.glob("coinbase_*.jsonl")) + sorted(RAW_DIR.glob("coinbase_*.json"))
        if not files:
            raise FileNotFoundError(
                f"No coinbase_*.jsonl files in {RAW_DIR}. "
                "Run producer/coinbase_collector.py or producer/generate_coinbase_sample_data.py first."
            )
        raw_file = max(files, key=lambda f: f.stat().st_mtime)

    print(f"Loading: {raw_file}")
    records: list[dict] = []
    with raw_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    df = pd.DataFrame(records)
    print(f"Loaded {len(df):,} raw records from {raw_file.name}")
    return df, raw_file


def clean_trades(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Coinbase-aware cleaning.

    Same intent as the streaming filter in
    ``streaming/spark_stream_kafka_coinbase_clean_aggregate.py``: enforce
    presence of the identifying columns, valid numeric ranges, parseable
    timestamps, and uniqueness of ``trade_id``.
    """
    initial_count = len(df)
    print("\n--- CLEANING STEPS ---")

    expected = ["source", "product_id", "trade_id", "price", "quantity", "trade_time", "side", "raw_type"]
    for col in expected:
        if col not in df.columns:
            df[col] = pd.NA

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["trade_id"] = pd.to_numeric(df["trade_id"], errors="coerce", downcast="integer")
    df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce", utc=True)

    before_nulls = len(df)
    df = df.dropna(subset=["product_id", "trade_id", "price", "quantity", "trade_time"])
    print(f"1. Nulls/parse failures removed: {before_nulls - len(df):,} rows")

    before_invalid = len(df)
    df = df[(df["price"] > 0) & (df["price"] < 1e9)]
    df = df[(df["quantity"] > 0) & (df["quantity"] < 1e9)]
    print(f"2. Invalid price/quantity removed: {before_invalid - len(df):,} rows")

    df["trade_value"] = df["price"] * df["quantity"]

    before_dup = len(df)
    df = df.drop_duplicates(subset=["product_id", "trade_id"], keep="first")
    print(f"3. Duplicates removed: {before_dup - len(df):,} rows")

    df = df.sort_values(["product_id", "trade_time"]).reset_index(drop=True)

    final_count = len(df)
    print(
        f"\nBefore cleaning: {initial_count:,} | After: {final_count:,} | Removed: {initial_count - final_count:,}"
    )
    return df


def save_cleaned(df: pd.DataFrame) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = CLEANED_DIR / f"coinbase_cleaned_trades_{timestamp}.csv"
    df.to_csv(out_file, index=False)
    print(f"Saved cleaned data to {out_file}")
    return out_file


def run_cleaning(raw_file: Path | None = None) -> tuple[pd.DataFrame, Path]:
    df, _ = load_raw_data(raw_file)
    cleaned = clean_trades(df)
    out = save_cleaned(cleaned)
    return cleaned, out


if __name__ == "__main__":
    cleaned, _ = run_cleaning()
    print("\nSample of cleaned data:")
    print(cleaned.head(10))
