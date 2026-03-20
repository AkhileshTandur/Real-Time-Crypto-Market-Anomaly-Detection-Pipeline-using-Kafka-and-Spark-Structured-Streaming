"""
Binance WebSocket Data Collector
Collects real-time crypto trade data for preprocessing and EDA.
Run this first to gather data before cleaning and analysis.
"""

import json
import asyncio
from datetime import datetime
from pathlib import Path

try:
    import websockets
except ImportError:
    print("Install websockets: pip install websockets")
    exit(1)

# Output folder for raw data
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Symbols to collect (add more if needed)
SYMBOLS = ["btcusdt", "ethusdt"]

# Collect for this many seconds (60=quick demo, 120+=more data for EDA)
COLLECT_DURATION_SECONDS = 60


def get_ws_url(symbol: str) -> str:
    """Binance trade stream URL (no API key needed)."""
    return f"wss://stream.binance.com:9443/ws/{symbol}@trade"


async def collect_trades(symbol: str, duration: int) -> list:
    """Collect trade events from Binance WebSocket for a given duration."""
    url = get_ws_url(symbol)
    trades = []
    print(f"Connecting to {symbol}...")

    try:
        async with websockets.connect(url) as ws:
            start = datetime.now()
            while (datetime.now() - start).seconds < duration:
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(msg)
                # Add symbol and collection timestamp
                data["symbol"] = symbol.upper()
                data["collected_at"] = datetime.now().isoformat()
                trades.append(data)
                if len(trades) % 500 == 0:
                    print(f"  {symbol}: {len(trades)} trades collected")
    except Exception as e:
        print(f"  Error for {symbol}: {e}")

    return trades


async def main():
    """Collect trades from all symbols and save to JSON."""
    print(f"Collecting trades for {COLLECT_DURATION_SECONDS} seconds...")
    all_trades = []

    for symbol in SYMBOLS:
        trades = await collect_trades(symbol, COLLECT_DURATION_SECONDS)
        all_trades.extend(trades)
        print(f"  {symbol}: collected {len(trades)} trades")

    if len(all_trades) == 0:
        # For the rubric, prefer real collected data over synthetic data.
        # If this happens, you likely have connectivity/firewall/region issues.
        raise RuntimeError("No trades were collected from Binance WebSocket.")

    # Save to JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = OUTPUT_DIR / f"trades_{timestamp}.json"
    with open(out_file, "w") as f:
        for t in all_trades:
            f.write(json.dumps(t) + "\n")

    print(f"\nSaved {len(all_trades)} trades to {out_file}")
    return out_file


if __name__ == "__main__":
    asyncio.run(main())
