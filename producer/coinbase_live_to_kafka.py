"""Stream live Coinbase market updates directly into Kafka using the trade schema."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
import websockets


COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_TO_SYMBOL = {
    "BTC-USD": "BTCUSD",
    "ETH-USD": "ETHUSD",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap_servers", default="localhost:9092")
    parser.add_argument("--topic", default="crypto.trades")
    parser.add_argument("--products", default="BTC-USD,ETH-USD")
    parser.add_argument("--channel", choices=["ticker", "matches"], default="ticker")
    parser.add_argument("--duration_seconds", type=int, default=0, help="0 means run until Ctrl+C.")
    parser.add_argument("--max_events", type=int, default=0, help="0 means no limit.")
    parser.add_argument("--log_every", type=int, default=1, help="Print progress every N sent trades.")
    return parser.parse_args()


def iso_to_epoch_ms(value: str | None) -> int:
    if not value:
        return int(time.time() * 1000)
    normalized = value.replace("Z", "+00:00")
    return int(datetime.fromisoformat(normalized).timestamp() * 1000)


def coinbase_message_to_trade(message: dict) -> dict | None:
    msg_type = message.get("type")
    if msg_type not in {"ticker", "match", "last_match"}:
        return None

    product_id = message.get("product_id")
    symbol = PRODUCT_TO_SYMBOL.get(product_id, str(product_id or "").replace("-", ""))
    event_time_ms = iso_to_epoch_ms(message.get("time"))
    price = message.get("price")
    size = message.get("last_size") or message.get("size")

    if price is None or size is None:
        return None

    return {
        "e": "trade",
        "E": event_time_ms,
        "s": symbol,
        "t": int(message.get("trade_id", 0)),
        "p": str(price),
        "q": str(size),
        "T": event_time_ms,
        "m": message.get("side") == "sell",
        "source": "coinbase",
        "channel": msg_type,
    }


async def stream_live(args: argparse.Namespace) -> None:
    products = [product.strip().upper() for product in args.products.split(",") if product.strip()]
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        linger_ms=20,
        batch_size=16384,
    )

    subscribe = {
        "type": "subscribe",
        "product_ids": products,
        "channels": [args.channel],
    }

    sent = 0
    start = time.time()
    print(f"Connecting to Coinbase WebSocket: {COINBASE_WS_URL}")
    print(f"Streaming products={products} to Kafka topic='{args.topic}' @ {args.bootstrap_servers}")

    try:
        async with websockets.connect(COINBASE_WS_URL, ping_interval=20, ping_timeout=20) as websocket:
            await websocket.send(json.dumps(subscribe))

            while True:
                if args.duration_seconds and time.time() - start >= args.duration_seconds:
                    break
                if args.max_events and sent >= args.max_events:
                    break

                raw = await websocket.recv()
                message = json.loads(raw)
                trade = coinbase_message_to_trade(message)
                if trade is None:
                    continue

                producer.send(args.topic, key=trade["s"], value=trade)
                sent += 1

                if sent % max(args.log_every, 1) == 0:
                    producer.flush()
                    elapsed = time.time() - start
                    rate = sent / max(elapsed, 1e-6)
                    print(f"  sent={sent:,} live trades, latest={trade['s']} price={trade['p']} (~{rate:.1f} events/sec)")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. Total live trades sent: {sent:,}")


def main() -> None:
    args = parse_args()
    asyncio.run(stream_live(args))


if __name__ == "__main__":
    main()
