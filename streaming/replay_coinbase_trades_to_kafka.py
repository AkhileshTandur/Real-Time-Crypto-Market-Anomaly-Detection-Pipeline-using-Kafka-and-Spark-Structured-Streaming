"""Replay normalized Coinbase trade JSONL files into Kafka.

Reads any ``coinbase_*.jsonl`` (live or synthetic) under ``--input_dir`` and
publishes one Kafka record per JSON object on the configured topic. The
Coinbase ``product_id`` is used as the Kafka message key so trades for the
same product land on the same partition (important for ordering when the
topic is later scaled to multiple partitions).
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--input_dir", required=True, help="Directory containing coinbase_*.jsonl files.")
    p.add_argument("--topic", default="coinbase.trades", help="Kafka topic name.")
    p.add_argument("--bootstrap_servers", default="localhost:9092", help="Kafka bootstrap servers.")
    p.add_argument(
        "--product_ids",
        default="BTC-USD,ETH-USD",
        help="Comma-separated Coinbase product ids to keep. Use 'ALL' to disable filtering.",
    )
    p.add_argument("--sleep_mode", choices=["none", "event_time"], default="none")
    p.add_argument(
        "--speed_factor",
        type=float,
        default=50.0,
        help="When sleep_mode=event_time, scale wall-clock delay by 1/speed_factor.",
    )
    p.add_argument("--max_sleep_ms", type=int, default=50, help="Cap sleep per record (ms).")
    p.add_argument("--max_events", type=int, default=0, help="0 = no limit.")
    return p.parse_args()


def list_jsonl_files(input_dir: Path) -> list[Path]:
    """Return Coinbase JSONL/JSON files. Skips other producers' files."""
    candidates = sorted(input_dir.glob("coinbase_*.jsonl")) + sorted(input_dir.glob("coinbase_*.json"))
    return candidates


def iter_json_lines(files: Iterable[Path]) -> Iterable[dict]:
    for fp in files:
        with fp.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def event_time_ms(obj: dict) -> Optional[int]:
    """Convert the normalized ISO-8601 ``trade_time`` to epoch milliseconds.

    Returned None means the record is unusable for event-time pacing.
    """
    ts = obj.get("trade_time")
    if not ts or not isinstance(ts, str):
        return None
    try:
        # Accept both '...Z' and '+00:00' forms.
        normalized = ts.replace("Z", "+00:00")
        return int(datetime.fromisoformat(normalized).timestamp() * 1000)
    except Exception:
        return None


def main() -> None:
    args = parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input dir not found: {input_dir}")

    files = list_jsonl_files(input_dir)
    if not files:
        raise FileNotFoundError(
            f"No coinbase_*.jsonl files found in {input_dir}. "
            "Run producer/coinbase_collector.py or producer/generate_coinbase_sample_data.py first."
        )

    total_bytes = sum(fp.stat().st_size for fp in files)
    total_gb = total_bytes / (1024**3)
    print(f"Found {len(files)} Coinbase file(s), total size ~{total_gb:.4f} GB")
    if total_bytes < 1 * 1024**3:
        print("WARNING: input is small; suitable for replay/velocity tests, not high-volume claims.")

    keep_all = args.product_ids.strip().upper() == "ALL"
    products_set = (
        None if keep_all else {p.strip().upper() for p in args.product_ids.split(",") if p.strip()}
    )

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: v,
        key_serializer=lambda v: v,
        linger_ms=20,
        batch_size=16384,
    )

    sent = 0
    skipped = 0
    prev_ts = None
    start_wall = time.time()

    print(f"Replaying to Kafka topic='{args.topic}' @ '{args.bootstrap_servers}'")
    print(f"sleep_mode={args.sleep_mode}")

    for obj in iter_json_lines(files):
        product_id = obj.get("product_id")
        if not product_id:
            skipped += 1
            continue
        if products_set is not None and product_id.upper() not in products_set:
            skipped += 1
            continue

        ts = event_time_ms(obj)
        if args.sleep_mode == "event_time" and ts is not None and prev_ts is not None:
            delay_ms = (ts - prev_ts) / float(args.speed_factor)
            if delay_ms < 0:
                delay_ms = 0
            delay_ms = min(delay_ms, args.max_sleep_ms)
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)
        if ts is not None:
            prev_ts = ts

        msg = json.dumps(obj).encode("utf-8")
        key = product_id.upper().encode("utf-8")
        producer.send(args.topic, key=key, value=msg)
        sent += 1

        if args.max_events and sent >= args.max_events:
            break
        if sent % 5000 == 0:
            producer.flush()
            elapsed = time.time() - start_wall
            rate = sent / max(elapsed, 1e-6)
            print(f"  sent={sent:,} skipped={skipped:,} (~{rate:,.0f} events/sec)")

    producer.flush()
    producer.close()
    elapsed = time.time() - start_wall
    rate = sent / max(elapsed, 1e-6)
    print(
        f"Done. Total sent: {sent:,}, skipped: {skipped:,}, in {elapsed:.1f}s "
        f"(~{rate:,.0f} events/sec)"
    )


if __name__ == "__main__":
    main()
