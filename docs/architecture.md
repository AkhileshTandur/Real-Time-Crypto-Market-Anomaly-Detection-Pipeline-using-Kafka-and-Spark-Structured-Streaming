# Architecture

## Components

| Component | Role |
| --- | --- |
| Binance WebSocket collector | Captures live trade events and writes JSONL files |
| Historical replay producer | Streams stored JSONL files into Kafka for repeatable demos |
| Kafka | Buffers high-velocity trade events and decouples producers from Spark |
| Spark Structured Streaming | Parses, cleans, windows, aggregates, and flags anomalies |
| EDA scripts | Convert cleaned and streaming outputs into presentation charts |
| Batch anomaly scorer | Validates anomaly behavior with rolling symbol-specific baselines |

## Data Contract

Input records follow the Binance trade shape:

```json
{
  "e": "trade",
  "E": 1710000000000,
  "s": "BTCUSDT",
  "t": 123456,
  "p": "97000.12",
  "q": "0.025",
  "T": 1710000000000,
  "m": false
}
```

The normalized schema used downstream:

| Field | Meaning |
| --- | --- |
| symbol | Trading pair |
| trade_id | Exchange trade identifier |
| price | Trade price in USDT |
| quantity | Base asset quantity |
| trade_time | Event timestamp |
| trade_value | `price * quantity` |

## Streaming Output

Spark writes one row per symbol per time window:

| Field | Meaning |
| --- | --- |
| window_start / window_end | Event-time aggregation window |
| symbol | Trading pair |
| avg_price / min_price / max_price | Price summary |
| price_spread_pct | Intrawindow price movement |
| volume | Sum of traded quantity |
| notional_volume | Sum of trade value in USDT |
| max_trade_value | Largest single trade value |
| trade_count | Number of trades in the window |
| is_anomaly | Boolean anomaly flag |
| anomaly_reason | Triggered rule names |
