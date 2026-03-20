"""
Generate sample Binance-style trade data for demo when WebSocket is unavailable
(e.g., HTTP 451 geo-restriction). Same schema as real Binance trade stream.
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Approximate prices (USDT)
BTC_PRICE = 97000
ETH_PRICE = 3500

# Generate ~2000 trades per symbol
NUM_TRADES = 2000


def generate_trades(symbol: str, base_price: float, count: int) -> list:
    """Generate realistic-looking trade records."""
    trades = []
    ts = int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)
    trade_id = 100000000 + random.randint(0, 99999999)

    for _ in range(count):
        # Random price within ±0.5%
        price = round(base_price * (1 + random.uniform(-0.005, 0.005)), 2)
        quantity = round(random.uniform(0.0001, 2.0), 6)
        ts += random.randint(100, 5000)
        trade_id += 1
        trades.append({
            "e": "trade",
            "s": symbol.upper(),
            "t": trade_id,
            "p": str(price),
            "q": str(quantity),
            "T": ts,
            "m": random.choice([True, False]),
        })
    return trades


def add_bad_records(trades: list, count: int = 50) -> list:
    """Add records with quality issues for cleaning demo (nulls, invalid values, dupes)."""
    bad = []
    for i in range(count):
        t = trades[i % len(trades)].copy()
        if i % 5 == 0:
            t["p"] = ""  # null price
        elif i % 5 == 1:
            t["q"] = "-1"  # invalid quantity
        elif i % 5 == 2:
            t["p"] = "0"  # zero price
        elif i % 5 == 3:
            t["t"] = trades[0]["t"]  # duplicate trade_id
        else:
            t["s"] = None  # null symbol
        bad.append(t)
    return trades + bad


def main():
    print("Generating sample trade data (Binance format)...")
    all_trades = []
    all_trades.extend(generate_trades("btcusdt", BTC_PRICE, NUM_TRADES))
    all_trades.extend(generate_trades("ethusdt", ETH_PRICE, NUM_TRADES))
    all_trades = add_bad_records(all_trades)  # Add ~50 bad records for cleaning demo
    random.shuffle(all_trades)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = OUTPUT_DIR / f"trades_{timestamp}.json"
    with open(out_file, "w") as f:
        for t in all_trades:
            f.write(json.dumps(t) + "\n")

    print(f"Saved {len(all_trades)} sample trades to {out_file}")


if __name__ == "__main__":
    main()
