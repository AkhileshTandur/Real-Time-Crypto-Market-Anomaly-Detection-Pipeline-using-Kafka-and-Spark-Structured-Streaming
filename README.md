# Real-Time Crypto Market Anomaly Detection

This project streams crypto trade data through Kafka, processes it with Spark Structured Streaming, and flags unusual market activity using short-window price, volume, and trade-count signals.

The pipeline is designed around live market data. Binance trade events arrive continuously, Kafka buffers the event stream, and Spark turns raw ticks into cleaned aggregates that can be monitored or analyzed later.

## What It Does

- collects Binance trade events for pairs such as `BTCUSDT` and `ETHUSDT`
- replays stored JSONL trade files into Kafka for repeatable runs
- parses and cleans trade events in Spark
- computes per-symbol time-window metrics
- flags suspicious windows based on price spread, large trades, and traffic spikes
- generates offline EDA charts and anomaly-score files

## Architecture

```text
Binance WebSocket / JSONL replay
        |
        v
Kafka topic: binance.trades
        |
        v
Spark Structured Streaming
        |
        v
Windowed market metrics + anomaly flags
        |
        v
Charts, logs, and downstream analysis
```

## Repository Layout

```text
producer/
  binance_collector.py                 Live Binance WebSocket collector
  generate_sample_data.py              Local Binance-style data generator

streaming/
  replay_binance_trades_to_kafka.py    JSONL-to-Kafka replay producer
  spark_stream_kafka_binance_clean_aggregate.py

processing/
  data_cleaning.py                     Batch cleaning
  anomaly_detection.py                 Offline rolling z-score anomaly scoring

eda/
  eda_analysis.py                      Charts from cleaned trade files
  eda_from_stream_aggregates.py        Charts from Spark aggregate output

dashboard/
  index.html                           Browser-based workflow visualizer

docs/
  architecture.md                      Data contract and component overview
  runbook.md                           Runtime checklist
  ui_walkthrough.md                    UI guide for Kafka, Spark, and Docker
```

## Requirements

- Docker Desktop
- Python 3.10+
- Java 17 if running Spark outside Docker

Install Python dependencies:

```powershell
py -m pip install -r requirements.txt
```

## Run With Docker

Start Kafka, Redpanda Console, and Spark:

```powershell
docker compose up -d
```

Open the UIs:

```text
Kafka UI: http://localhost:8080
Spark UI: http://localhost:4040
```

Create the Kafka topic if it does not exist:

```powershell
docker exec crypto-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic binance.trades --partitions 1 --replication-factor 1
```

Replay trade data into Kafka:

```powershell
py streaming\replay_binance_trades_to_kafka.py --input_dir data\raw --topic binance.trades --bootstrap_servers localhost:9092 --symbols BTCUSDT,ETHUSDT --sleep_mode none --max_events 2000
```

Spark reads from `binance.trades` and prints windowed aggregates from inside the Spark container. Use Spark UI to inspect jobs, stages, executors, and streaming activity.

## Run Locally

The local Spark helper sets Java and PySpark paths for Windows:

```powershell
.\scripts\start_spark_streaming.ps1
```

Docker is still the recommended runtime on Windows because Spark checkpointing is more reliable inside the Linux container.

## Offline Analysis

Run the local batch pipeline:

```powershell
py run_all.py
```

This performs cleaning, EDA chart generation, and offline anomaly scoring.

Important outputs:

```text
output/
data/anomalies/latest_trade_anomaly_scores.csv
```

## Stream Processing

Spark normalizes the Binance fields into a readable schema:

```text
s -> symbol
t -> trade_id
p -> price
q -> quantity
T -> trade_time
```

The streaming job filters invalid records, derives `trade_value`, applies event-time windows, and calculates:

```text
avg_price
min_price
max_price
price_spread_pct
volume
notional_volume
max_trade_value
trade_count
is_anomaly
anomaly_reason
```

## Anomaly Signals

A window is flagged when one or more configured rules trigger:

- price spread exceeds the threshold
- a single trade has unusually high notional value
- trade count is above the traffic-spike threshold

The batch anomaly scorer adds rolling z-score checks per symbol for offline validation.

## Visual Workflow

Open this file in a browser:

```text
dashboard/index.html
```

It shows the data flow from ingestion to Kafka, Spark processing, and anomaly output. It is a lightweight visualizer for explaining the system without touching the running services.

## Notes

- The committed data is intentionally small. Longer live collection or larger replay files give better baselines.
- Binance access can vary by network or region. The replay path keeps the pipeline testable even when live collection is unavailable.
- For durable storage, switch the Spark sink from console/CSV to Parquet, Delta Lake, or another warehouse-backed target.
