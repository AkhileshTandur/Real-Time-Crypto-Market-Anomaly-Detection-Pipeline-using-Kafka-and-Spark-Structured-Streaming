"""Replay Binance JSONL trade files into Kafka."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Iterable, Optional

from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input_dir", required=True, help="Directory containing historical trade json files.")
    p.add_argument("--topic", default="binance.trades", help="Kafka topic name.")
    p.add_argument("--bootstrap_servers", default="localhost:9092", help="Kafka bootstrap servers.")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT", help="Comma-separated symbol filters. Use 'ALL' to disable.")
    p.add_argument("--sleep_mode", choices=["none", "event_time"], default="none")
    p.add_argument("--speed_factor", type=float, default=50.0, help="How much to speed up event-time delays.")
    p.add_argument("--max_sleep_ms", type=int, default=50, help="Cap sleep time per message.")
    p.add_argument("--max_events", type=int, default=0, help="0 means no limit.")
    return p.parse_args()


def iter_json_lines(files: Iterable[Path]) -> Iterable[dict]:
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def guess_symbol(obj: dict) -> Optional[str]:
    if "s" in obj and obj["s"]:
        return str(obj["s"])
    if "symbol" in obj and obj["symbol"]:
        return str(obj["symbol"])
    return None


def guess_event_time_ms(obj: dict) -> Optional[int]:
    for k in ("T", "E"):
        if k in obj and obj[k] is not None:
            try:
                return int(obj[k])
            except Exception:
                pass
    return None


def main() -> None:
    args = parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input dir not found: {input_dir}")

    files = sorted(input_dir.glob("*.json")) + sorted(input_dir.glob("*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No .json/.jsonl files found in: {input_dir}")

    total_bytes = sum(fp.stat().st_size for fp in files)
    total_gb = total_bytes / (1024**3)
    print(f"Found {len(files)} files, total size ~{total_gb:.2f} GB")
    if total_bytes < 5 * 1024**3:
        print("WARNING: input is small; use this for replay/velocity tests, not volume claims.")

    symbols_set = None
    if args.symbols.strip().upper() != "ALL":
        symbols_set = set(s.strip().upper() for s in args.symbols.split(",") if s.strip())

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: v,
        key_serializer=lambda v: v,
        linger_ms=20,
        batch_size=16384,
    )

    prev_ts = None
    sent = 0
    start_wall = time.time()

    print(f"Replaying to Kafka topic='{args.topic}' @ '{args.bootstrap_servers}'")
    print(f"sleep_mode={args.sleep_mode}")

    for obj in iter_json_lines(files):
        sym = guess_symbol(obj)
        if sym is None:
            continue
        if symbols_set is not None and sym.upper() not in symbols_set:
            continue

        ts = guess_event_time_ms(obj)
        if args.sleep_mode == "event_time" and ts is not None and prev_ts is not None:
            delay_ms = (ts - prev_ts) / float(args.speed_factor)
            if delay_ms < 0:
                delay_ms = 0
            delay_ms = min(delay_ms, args.max_sleep_ms)
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)
        prev_ts = ts if ts is not None else prev_ts

        msg = json.dumps(obj).encode("utf-8")
        key = sym.upper().encode("utf-8")
        producer.send(args.topic, key=key, value=msg)

        sent += 1
        if args.max_events and sent >= args.max_events:
            break
        if sent % 5000 == 0:
            producer.flush()
            elapsed = time.time() - start_wall
            rate = sent / max(elapsed, 1e-6)
            print(f"  sent={sent:,} (~{rate:.0f} events/sec)")

    producer.flush()
    producer.close()
    elapsed = time.time() - start_wall
    print(f"Done. Total sent: {sent:,} in {elapsed:.1f}s")


if __name__ == "__main__":
    main()

