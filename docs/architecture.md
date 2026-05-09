# Architecture

## Components

| Component | Role |
| --- | --- |
| Coinbase WebSocket collector | Captures live `match` events from `wss://ws-feed.exchange.coinbase.com` and writes normalized JSONL |
| Synthetic Coinbase generator | Produces Coinbase-format records with controlled dirty data for repeatable runs |
| Replay producer | Streams normalized JSONL into Kafka topic `coinbase.trades` |
| Kafka | Buffers high-velocity trade events and decouples producers from Spark |
| Spark Structured Streaming | Parses, cleans, windows, aggregates, and flags anomalies |
| Delta Lake sink | ACID storage with time travel for the windowed aggregates |
| EDA scripts | Convert cleaned and streaming outputs into charts |
| Batch rule-based scorer | Per-product rolling z-scores for offline validation |
| ML scorer | IsolationForest for unsupervised anomaly detection and rule-vs-ML comparison |

## Data Contract (normalized Coinbase trade record)

```json
{
  "source": "coinbase",
  "product_id": "BTC-USD",
  "trade_id": 123456,
  "price": "97000.50",
  "quantity": "0.123",
  "trade_time": "2026-05-08T12:34:56.123456Z",
  "side": "buy",
  "collected_at": "2026-05-08T12:34:56.500000Z",
  "raw_type": "match"
}
```

## Streaming Output (Delta / CSV / console)

Spark writes one row per product per time window:

| Field | Meaning |
| --- | --- |
| `window_start` / `window_end` | Event-time aggregation window |
| `product_id` | Coinbase product, e.g. `BTC-USD` |
| `avg_price` / `min_price` / `max_price` | Price summary in USD |
| `price_spread_pct` | Intra-window price movement |
| `total_quantity` | Sum of base asset traded |
| `total_trade_value` | Sum of trade value in USD |
| `max_trade_value` | Largest single trade value in the window |
| `trade_count` | Number of trades in the window |
| `is_anomaly` | Boolean anomaly flag |
| `anomaly_reason` | Triggered rule names (`price_spread`, `large_trade`, `traffic_spike`) |
