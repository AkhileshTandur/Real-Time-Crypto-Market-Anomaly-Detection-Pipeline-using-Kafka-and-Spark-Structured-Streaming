# Real-Time Crypto Market Anomaly Detection

This project streams live cryptocurrency trades through Kafka, processes them with Spark Structured Streaming, and flags unusual market activity using short-window price, volume, and trade-count signals.

The live source is Coinbase Exchange public WebSocket market data. Trade events are converted into a compact exchange-neutral schema, sent to Kafka, and consumed by Spark in real time.

## What It Does

- collects live Coinbase trade events for products such as `BTC-USD` and `ETH-USD`
- publishes live trade records directly into Kafka topic `crypto.trades`
- parses and cleans trade events in Spark
- computes per-symbol event-time window metrics
- flags suspicious windows based on price spread, large trades, and traffic spikes
- generates offline EDA charts and anomaly-score files for dashboard visualization

## Architecture

```text
Coinbase WebSocket
        |
        v
Kafka topic: crypto.trades
        |
        v
Spark Structured Streaming
        |
        v
Windowed market metrics + anomaly flags
        |
        v
Charts, logs, and dashboard analysis
```

## Repository Layout

```text
producer/
  coinbase_live_to_kafka.py            Live Coinbase WebSocket to Kafka producer
  generate_sample_data.py              Local crypto data generator

streaming/
  spark_stream_kafka_crypto_clean_aggregate.py

processing/
  data_cleaning.py                     Batch cleaning
  anomaly_detection.py                 Offline rolling z-score anomaly scoring

eda/
  eda_analysis.py                      Charts from cleaned trade files
  eda_from_stream_aggregates.py        Charts from Spark aggregate output

dashboard/
  index.html                           Browser-based workflow visualizer
  build_dashboard_data.py              Builds dashboard/data/dashboard_data.json

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

## Run Live Streaming

Start Kafka, Redpanda Console, and Spark:

```powershell
docker compose up -d
```

Open the UIs:

```text
Kafka UI: http://localhost:8080
Spark UI: http://localhost:4040
```

Send live Coinbase trades to Kafka:

```powershell
py producer\coinbase_live_to_kafka.py --topic crypto.trades --bootstrap_servers localhost:9092 --products BTC-USD,ETH-USD
```

For a short demo:

```powershell
py producer\coinbase_live_to_kafka.py --topic crypto.trades --bootstrap_servers localhost:9092 --products BTC-USD,ETH-USD --duration_seconds 120
```

Spark reads from `crypto.trades` and prints windowed aggregates from inside the Spark container. Use Spark UI to inspect jobs, stages, executors, and streaming activity.

## Run Locally

The local Spark helper sets Java and PySpark paths for Windows:

```powershell
.\scripts\start_spark_streaming.ps1
```

Docker is still the recommended runtime on Windows because Spark checkpointing is more reliable inside the Linux container.

## Offline Analysis

Run the local batch analysis pipeline:

```powershell
py run_all.py
```

This performs cleaning, EDA chart generation, and offline anomaly scoring from files under `data/raw`.

Important outputs:

```text
output/
data/anomalies/latest_trade_anomaly_scores.csv
```

## Stream Processing

Spark normalizes compact exchange fields into a readable schema:

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

Build the dashboard data from the latest cleaned trades and anomaly scores:

```powershell
py dashboard\build_dashboard_data.py
```

Then open this file in a browser:

```text
dashboard/index.html
```

The dashboard reads `dashboard/data/dashboard_data.json` and shows real project outputs: record counts, symbols, price history, aggregate metrics, and anomaly history.
