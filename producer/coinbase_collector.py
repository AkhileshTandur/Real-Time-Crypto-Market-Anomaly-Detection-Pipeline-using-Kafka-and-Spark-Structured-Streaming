"""Collect live Coinbase trade events from the public Exchange WebSocket feed.

We use the Coinbase Exchange ``matches`` channel:
  wss://ws-feed.exchange.coinbase.com

This feed is public, requires no API key for market data, and emits one
message per executed trade. We subscribe by sending::

    {"type": "subscribe", "product_ids": [...], "channels": ["matches"]}

and then read frames of the form::

    {"type": "match", "trade_id": ..., "price": "...", "size": "...",
     "side": "buy", "product_id": "BTC-USD",
     "time": "2026-05-08T12:34:56.123456Z", "sequence": ...}

Every frame is normalized into the project-wide schema:
    source        - always "coinbase"
    product_id    - e.g. BTC-USD
    trade_id      - integer/string from Coinbase
    price         - float
    quantity      - float (size in Coinbase parlance)
    trade_time    - ISO-8601 string from Coinbase ``time``
    side          - "buy" / "sell" / "unknown"
    collected_at  - local ISO-8601 timestamp when we received the frame
    raw_type      - "match" or "last_match" (initial snapshot frame)

Output:
    data/raw/coinbase_trades_<UTC_timestamp>.jsonl  (one JSON object per line)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError:
    print("Install websockets: pip install websockets")
    sys.exit(1)


PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

WS_URL = "wss://ws-feed.exchange.coinbase.com"
DEFAULT_PRODUCTS = ["BTC-USD", "ETH-USD"]
DEFAULT_DURATION = 60
DEFAULT_MAX_RECORDS = 0
RECONNECT_BACKOFF_SEC = 3.0
MAX_RECONNECTS = 5


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument(
        "--product_ids",
        default=",".join(DEFAULT_PRODUCTS),
        help="Comma-separated Coinbase product ids (default: BTC-USD,ETH-USD).",
    )
    p.add_argument(
        "--duration_seconds",
        type=int,
        default=DEFAULT_DURATION,
        help="Stop after this many seconds of collection (0 disables).",
    )
    p.add_argument(
        "--max_records",
        type=int,
        default=DEFAULT_MAX_RECORDS,
        help="Stop after this many records (0 disables).",
    )
    p.add_argument(
        "--output_dir",
        default=str(OUTPUT_DIR),
        help="Directory for the output JSONL file (default: data/raw).",
    )
    return p.parse_args()


def normalize(frame: dict) -> dict | None:
    """Convert a Coinbase WebSocket frame into the project's normalized schema.

    Returns None for frames that are not real trade events (subscribe acks,
    heartbeats, errors, etc.) so the caller can simply skip them.
    """
    raw_type = frame.get("type")
    if raw_type not in ("match", "last_match"):
        return None
    if frame.get("price") is None or frame.get("size") is None:
        return None
    side_raw = frame.get("side", "")
    side = side_raw if side_raw in ("buy", "sell") else "unknown"
    return {
        "source": "coinbase",
        "product_id": frame.get("product_id"),
        "trade_id": frame.get("trade_id"),
        "price": frame.get("price"),
        "quantity": frame.get("size"),
        "trade_time": frame.get("time"),
        "side": side,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "raw_type": raw_type,
    }


async def stream_loop(
    products: Iterable[str],
    out_path: Path,
    duration_seconds: int,
    max_records: int,
) -> int:
    """Open a single WebSocket session, subscribe, and write normalized records.

    Returns the number of records written. Any exception bubbles up so the
    outer reconnect loop can decide whether to retry.
    """
    products = list(products)
    written = 0
    started = datetime.now(timezone.utc)

    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
        print(f"connected to {WS_URL}")
        sub_msg = {
            "type": "subscribe",
            "product_ids": products,
            "channels": ["matches"],
        }
        await ws.send(json.dumps(sub_msg))
        print(f"subscribed to matches channel for {products}")

        with out_path.open("a", encoding="utf-8") as f:
            while True:
                if duration_seconds > 0:
                    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
                    if elapsed >= duration_seconds:
                        print(f"duration limit reached ({duration_seconds}s)")
                        break
                if max_records > 0 and written >= max_records:
                    print(f"max_records reached ({max_records})")
                    break

                raw = await asyncio.wait_for(ws.recv(), timeout=15.0)
                frame = json.loads(raw)
                record = normalize(frame)
                if record is None:
                    continue

                f.write(json.dumps(record) + "\n")
                written += 1
                if written % 250 == 0:
                    print(f"  wrote {written:,} records")

    return written


async def main() -> None:
    args = parse_args()
    products = [p.strip() for p in args.product_ids.split(",") if p.strip()]
    if not products:
        raise SystemExit("--product_ids must list at least one Coinbase product id")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"coinbase_trades_{timestamp}.jsonl"

    print(f"writing normalized trades to {out_path}")

    total_written = 0
    for attempt in range(1, MAX_RECONNECTS + 1):
        try:
            total_written += await stream_loop(
                products, out_path, args.duration_seconds, args.max_records
            )
            break
        except (ConnectionClosed, asyncio.TimeoutError, OSError) as exc:
            print(f"connection issue (attempt {attempt}/{MAX_RECONNECTS}): {exc}")
            if attempt == MAX_RECONNECTS:
                print("giving up after repeated failures")
                break
            await asyncio.sleep(RECONNECT_BACKOFF_SEC * attempt)
        except Exception as exc:
            print(f"unexpected error: {exc!r}")
            raise

    print(f"done. total records written: {total_written:,} -> {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
