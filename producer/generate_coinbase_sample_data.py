"""Generate synthetic Coinbase-format trade records for repeatable demos.

Output matches the normalized schema produced by ``coinbase_collector.py`` so
the rest of the pipeline (replay, Spark, cleaning, ML) cannot tell synthetic
records from real ones. Dirty records are deliberately injected so the
cleaning step has work to do during demos.

Output:
    data/raw/coinbase_synthetic_<UTC_timestamp>.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_PRODUCTS = ["BTC-USD", "ETH-USD"]
BASE_PRICES = {"BTC-USD": 97_000.0, "ETH-USD": 3_500.0}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--num_records", type=int, default=2000, help="Total clean records to generate.")
    p.add_argument(
        "--product_ids",
        default=",".join(DEFAULT_PRODUCTS),
        help="Comma-separated Coinbase product ids (default: BTC-USD,ETH-USD).",
    )
    p.add_argument(
        "--dirty_rate",
        type=float,
        default=0.025,
        help="Fraction of records to corrupt (default 2.5%%).",
    )
    p.add_argument(
        "--output_dir",
        default=str(OUTPUT_DIR),
        help="Directory for the JSONL file (default: data/raw).",
    )
    p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    return p.parse_args()


def _utc_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def generate_clean(products: Iterable[str], total: int) -> list[dict]:
    """Yield ``total`` synthetic Coinbase trade records.

    The price walks per product so that rolling z-scores in the downstream
    pipeline have a meaningful baseline; quantities span a few orders of
    magnitude so trade_value distribution is interesting for IsolationForest.
    """
    products = list(products)
    records: list[dict] = []
    base_ts_ms = int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp() * 1000)
    base_trade_id = 100_000_000 + random.randint(0, 99_999_999)

    prices = {p: BASE_PRICES.get(p, 1_000.0) for p in products}
    for i in range(total):
        product = products[i % len(products)]
        prices[product] *= 1.0 + random.uniform(-0.0015, 0.0015)
        price = round(prices[product], 2)
        quantity = round(random.uniform(0.0001, 2.5), 6)
        ts_ms = base_ts_ms + i * random.randint(50, 1500)
        record = {
            "source": "coinbase",
            "product_id": product,
            "trade_id": base_trade_id + i,
            "price": str(price),
            "quantity": str(quantity),
            "trade_time": _utc_iso(ts_ms),
            "side": random.choice(["buy", "sell"]),
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "raw_type": "match",
        }
        records.append(record)
    return records


DIRTY_KINDS = (
    "missing_price",
    "negative_quantity",
    "zero_price",
    "duplicate_trade_id",
    "null_product_id",
    "malformed_timestamp",
    "extreme_trade_value",
)


def inject_dirty(records: list[dict], rate: float) -> list[dict]:
    """Corrupt a fraction of the records with one of the known bad patterns.

    Cleaning code (``processing/coinbase_data_cleaning.py``) and the Spark
    streaming filter both demonstrate handling of these cases live.
    """
    if rate <= 0 or not records:
        return records
    n_bad = max(1, int(len(records) * rate))
    chosen = random.sample(range(len(records)), n_bad)
    for idx, kind in zip(chosen, [random.choice(DIRTY_KINDS) for _ in chosen]):
        bad = dict(records[idx])
        if kind == "missing_price":
            bad["price"] = ""
        elif kind == "negative_quantity":
            bad["quantity"] = "-1.0"
        elif kind == "zero_price":
            bad["price"] = "0"
        elif kind == "duplicate_trade_id":
            bad["trade_id"] = records[0]["trade_id"]
        elif kind == "null_product_id":
            bad["product_id"] = None
        elif kind == "malformed_timestamp":
            bad["trade_time"] = "not-a-date"
        elif kind == "extreme_trade_value":
            bad["price"] = "1e12"
            bad["quantity"] = "1e6"
        records[idx] = bad
    return records


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    products = [p.strip() for p in args.product_ids.split(",") if p.strip()]
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"coinbase_synthetic_{timestamp}.jsonl"

    print(f"Generating {args.num_records:,} synthetic Coinbase records for {products}")
    records = generate_clean(products, args.num_records)
    records = inject_dirty(records, args.dirty_rate)
    random.shuffle(records)

    with out_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    n_dirty = max(1, int(args.num_records * args.dirty_rate)) if args.dirty_rate > 0 else 0
    print(
        f"Wrote {len(records):,} records (~{n_dirty:,} dirty) to {out_path}"
    )


if __name__ == "__main__":
    main()
