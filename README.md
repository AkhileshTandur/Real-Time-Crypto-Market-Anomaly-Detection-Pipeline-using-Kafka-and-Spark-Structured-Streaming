# Real-Time Coinbase Market Anomaly Detection

This project streams Coinbase trade data through Kafka, processes it with Spark Structured Streaming, lands windowed aggregates in Delta Lake, and flags unusual market activity using both rule-based signals and an Isolation Forest model.

The pipeline supports live Coinbase WebSocket ingestion and, for reproducible demos, a Coinbase-format synthetic generator with controlled dirty data. Both real and synthetic records are normalized into one schema, replayed into Kafka, processed by Spark, stored in Delta Lake, and analyzed with scikit-learn.

## What It Does

- collects Coinbase trade events for products `BTC-USD` and `ETH-USD` from the public Exchange WebSocket (`wss://ws-feed.exchange.coinbase.com`, `matches` channel)
- generates Coinbase-format synthetic trade data with realistic dirty records for repeatable runs
- replays normalized JSONL files into Kafka topic `coinbase.trades`
- parses, cleans, windows, and aggregates trades in Spark Structured Streaming
- writes ACID Delta Lake commits per micro-batch (with time travel)
- flags suspicious windows by spread %, large trades, and traffic spikes
- runs an Isolation Forest on cleaned data and compares ML anomalies vs rule-based anomalies
- generates EDA charts and ML diagnostic charts under `output/`

## Architecture

```text
Coinbase WebSocket  /  synthetic Coinbase data
              |
              v
    data/raw/coinbase_*.jsonl
              |
              v
   replay_coinbase_trades_to_kafka.py
              |
              v
   Kafka topic: coinbase.trades
              |
              v
   Spark Structured Streaming
   (parse -> clean -> window -> rule flags)
              |
              v
   Delta Lake sink: data/stream/coinbase_aggregates_delta
              |
              +--> Spark UI  (port 4040)
              +--> Kafka UI  (port 8080)
              +--> read_coinbase_delta_output.py (schema, history, time travel)

   Offline batch path:
     coinbase_data_cleaning.py -> data/cleaned/coinbase_cleaned_trades_*.csv
       -> anomaly_detection.py            (rolling z-score per product_id)
       -> ml_anomaly_detection.py         (IsolationForest + comparison)
```

## Repository Layout

```text
producer/
  coinbase_collector.py                     Live Coinbase WebSocket collector
  generate_coinbase_sample_data.py          Synthetic Coinbase trade generator (with dirty records)

streaming/
  replay_coinbase_trades_to_kafka.py        JSONL-to-Kafka replay producer
  spark_stream_kafka_coinbase_clean_aggregate.py
                                            Spark Structured Streaming job (Delta/CSV/console sinks)

processing/
  coinbase_data_cleaning.py                 Batch cleaning of Coinbase JSONL
  anomaly_detection.py                      Rule-based per-product rolling z-score
  ml_anomaly_detection.py                   IsolationForest + rule-vs-ML comparison
  read_coinbase_delta_output.py             Delta schema, history, time travel

eda/
  eda_analysis.py                           Charts from cleaned Coinbase data
  eda_from_stream_aggregates.py             Charts from Spark CSV aggregates

dashboard/
  index.html                                Browser-based pipeline visualizer (simulation)

docs/
  architecture.md                           Component contract and data flow
  runbook.md                                Operational checklist
  ui_walkthrough.md                         Kafka UI / Spark UI / dashboard guide
```

## Requirements

- Docker Desktop
- Python 3.10+
- Java 17 if running Spark outside Docker

Install Python dependencies:

```bash
python -m pip install -r requirements.txt
```

## Quickstart with Docker

Start Kafka and Kafka UI:

```bash
docker compose up -d kafka kafka-ui
```

Open the UIs:

```text
Kafka UI: http://localhost:8080
Spark UI: http://localhost:4040
```

Create the Kafka topic:

```bash
docker exec crypto-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic coinbase.trades --partitions 1 --replication-factor 1
```

Generate Coinbase data (synthetic by default):

```bash
python producer/generate_coinbase_sample_data.py --num_records 5000
# or, real Coinbase WebSocket:
python producer/coinbase_collector.py --duration_seconds 60
```

Start the Spark streaming job with the Delta Lake sink:

```bash
docker compose run --rm --service-ports spark-streaming \
  /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
  streaming/spark_stream_kafka_coinbase_clean_aggregate.py \
  --bootstrap_servers kafka:29092 \
  --topic coinbase.trades \
  --delta_path data/stream/coinbase_aggregates_delta \
  --checkpoint_path data/stream/checkpoints/coinbase_agg \
  --window_seconds 60 --watermark_seconds 120 \
  --sink delta
```

Replay records into Kafka:

```bash
python streaming/replay_coinbase_trades_to_kafka.py \
  --input_dir data/raw \
  --topic coinbase.trades \
  --bootstrap_servers localhost:9092 \
  --product_ids BTC-USD,ETH-USD \
  --sleep_mode none \
  --max_events 5000
```

Verify the Delta table (schema, sample anomalies, history, time travel):

```bash
docker exec crypto-spark-streaming /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  processing/read_coinbase_delta_output.py \
  --delta_path data/stream/coinbase_aggregates_delta
```

Run the offline batch + ML pipeline:

```bash
python run_all.py
```

This runs cleaning, EDA, rule-based anomaly scoring, and Isolation Forest ML.

## Normalized Coinbase Schema

Both the live collector and the synthetic generator emit records in this shape:

| Field | Meaning |
| --- | --- |
| `source` | Always `coinbase` |
| `product_id` | Coinbase product, e.g. `BTC-USD` |
| `trade_id` | Numeric trade id from Coinbase |
| `price` | Trade price in USD (string in JSON, cast to double in Spark) |
| `quantity` | Base asset size (string in JSON, cast to double in Spark) |
| `trade_time` | ISO-8601 UTC timestamp of the trade |
| `side` | `buy`, `sell`, or `unknown` |
| `collected_at` | Local UTC timestamp when the record was captured |
| `raw_type` | `match` or `last_match` |

## Stream Output

Spark writes one row per product per window:

```text
window_start  window_end  product_id
avg_price  min_price  max_price  price_spread_pct
total_quantity  total_trade_value  max_trade_value  trade_count
is_anomaly  anomaly_reason
```

## Anomaly Signals

Rule-based (in stream and offline):

- price spread within the window exceeds `--spread_threshold_pct`
- a single trade has notional value at or above `--large_trade_value`
- trade count in the window is at or above `--high_trade_count`

`anomaly_reason` concatenates triggered labels (`price_spread`, `large_trade`, `traffic_spike`).

ML (offline):

- Isolation Forest trained on `[price, quantity, trade_value, price_zscore, trade_value_zscore]`
- contamination = 0.05, 100 estimators, standardized features
- `coinbase_ml_comparison.png` shows overlap with the rule-based output
- Feature importance is a correlation proxy (sklearn IsolationForest has no native importances)

## Live vs Synthetic Data

- The live Coinbase collector writes real trades to `data/raw/coinbase_trades_*.jsonl`.
- The synthetic generator writes Coinbase-shape records (with controlled dirty data) to `data/raw/coinbase_synthetic_*.jsonl`.
- Both file types are picked up by `replay_coinbase_trades_to_kafka.py` and `coinbase_data_cleaning.py` automatically.

For a presentation: be explicit about which one is feeding the demo. Coinbase WebSocket access can be blocked on some networks, so the synthetic generator is a safe fallback.

## Team

- (add your team members here, one per line: Name &mdash; role)

## Notes / Limitations

- Demo runs may use synthetic Coinbase-format records when network access to `ws-feed.exchange.coinbase.com` is restricted; live ingestion should be tested before the presentation.
- Default committed data is small. Run a longer collection or replay if you want richer baselines.
- For durable storage in production, write the Delta sink to cloud object storage (S3 / ADLS / GCS) and consider scheduling Delta `OPTIMIZE` and `VACUUM`.
